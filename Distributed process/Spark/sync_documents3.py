from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
import subprocess

# ========= Paramètres =========
HDFS_INPUT = "hdfs:///tmp/documents_for_download.jsonl"
HDFS_OUTPUT_DIR = "hdfs:///user/vagrant/docs_downloaded_hdfs"   
CLIENT_ID = "073a2f7becdc35f7d1e7430b482db6e6"
API_URL_TPL = "https://api.mwater.co/v3/document_file/download/{doc_id}?client={client_id}&share="
HDFS_BIN = "/usr/local/hadoop/bin/hdfs"


MAX_RETRIES = 6
BASE_DELAY = 1.0
MAX_DELAY = 30.0

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    # "Authorization": "Bearer <TOKEN>",
}

def hdfs_mkdir_p(dir_path: str) -> None:
    subprocess.run([HDFS_BIN, "dfs", "-mkdir", "-p", dir_path], check=True)



SKIP_EXTS = {
    "zip", "qgz", "gsheet", "gslides",
    "xls", "xlsx", "xlsm",
    "jpg", "jpeg", "png", "rar"
}



def main():
    spark = SparkSession.builder.appName("DownloadDocsToHDFS").getOrCreate()

    # 1) Lire le JSONL depuis HDFS (fichier ou dossier)
    df = spark.read.json(HDFS_INPUT)

    # 2) Normaliser / sélectionner colonnes utiles
    df = (
        df.select("id", "extension")
          .where(col("id").isNotNull() & col("extension").isNotNull())
          .withColumn("extension", lower(trim(col("extension"))))
    )

    # 3) Créer le dossier de sortie en HDFS (driver)
    hdfs_mkdir_p(HDFS_OUTPUT_DIR)

    # 4) Traitement par partition (executors)
    def process_partition(rows_iter):
        import requests, time, random, subprocess

        def should_retry(status_code):
            if status_code is None:
                return True
            if status_code in (408, 429):
                return True
            return 500 <= status_code <= 599

        def exponential_backoff(attempt):
            delay = min(MAX_DELAY, BASE_DELAY * (2 ** attempt))
            return random.uniform(0, delay)

        def download_with_retries(session, url, headers):
            last_status = None
            last_exc = None
            for attempt in range(MAX_RETRIES):
                try:
                    resp = session.post(url, headers=headers, timeout=60)
                    last_status = resp.status_code
                    if resp.status_code == 200:
                        return resp.content
                    if should_retry(resp.status_code):
                        delay = exponential_backoff(attempt)
                        print("HTTP", resp.status_code, "- retry dans", f"{delay:.1f}s")
                        time.sleep(delay)
                        continue
                    resp.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    last_exc = e
                    delay = exponential_backoff(attempt)
                    print("Exception réseau:", e, "- retry dans", f"{delay:.1f}s")
                    time.sleep(delay)
            if last_exc:
                raise last_exc
            raise RuntimeError(f"Téléchargement échoué, dernier code HTTP: {last_status}")

        def hdfs_put_bytes(data, hdfs_dest_path):
            proc = subprocess.Popen(
                [HDFS_BIN, "dfs", "-put", "-f", "-", hdfs_dest_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = proc.communicate(input=data)
            if proc.returncode != 0:
                raise RuntimeError(
                    "'hdfs dfs -put' a échoué pour {}:\nSTDOUT: {}\nSTDERR: {}".format(
                        hdfs_dest_path,
                        (stdout.decode("utf-8", "ignore") if isinstance(stdout, bytes) else stdout),
                        (stderr.decode("utf-8", "ignore") if isinstance(stderr, bytes) else stderr),
                    )
                )

        session = requests.Session()
        processed = 0
        for row in rows_iter:
            doc_id = row["id"]
            ext = row["extension"]
            if not doc_id or not ext:
                print("Ligne ignorée (id ou extension manquants).")
                continue

            if ext.lower() in SKIP_EXTS:
                print(f"Fichier {doc_id}.{ext} ignoré (extension non supportée).")
                continue

            filename = f"{doc_id}.{ext}"
            hdfs_dest = f"{HDFS_OUTPUT_DIR}/{filename}"
            url = API_URL_TPL.format(doc_id=doc_id, client_id=CLIENT_ID)
            print("Téléchargement de", filename, "vers", hdfs_dest)

            try:
                content = download_with_retries(session, url, HEADERS)
                hdfs_put_bytes(content, hdfs_dest)
                processed += 1
                print("OK", hdfs_dest)
            except Exception as e:
                print("Échec pour", filename, ":", str(e))
        print("Partition terminée, fichiers traités:", processed)

    # Déclenche l’exécution parallèle (side-effects)
    df.rdd.foreachPartition(process_partition)

    spark.stop()
    print("Terminé.")

if __name__ == "__main__":
    main()
