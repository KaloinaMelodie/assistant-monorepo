from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import subprocess

HDFS_BIN = "/usr/local/hadoop/bin/hdfs"

spark = SparkSession.builder \
    .appName("DeleteOutputDocs") \
    .enableHiveSupport() \
    .getOrCreate()

id_schema = StructType().add("id", StringType())

try:
    df_raw = spark.read.text("hdfs:///tmp/consoles/to_delete.json")

    df_parsed = df_raw.withColumn("data", from_json(col("value"), id_schema))
    df_id = df_parsed.select(col("data.id").alias("id")).dropna()

    ids_to_delete = [row["id"] for row in df_id.collect()]

except Exception as e:
    print(f"Erreur lors de la lecture : {e}")
    ids_to_delete = []

if not ids_to_delete:
    print("Aucun fichier à supprimer.")
else:
    print(f"Suppression de {len(ids_to_delete)} fichiers...")
    for doc_id in ids_to_delete:
        file_path = f"hdfs:///user/vagrant/output_docs_console_hdfs/{doc_id}.txt"
        subprocess.run([HDFS_BIN, "dfs", "-rm", "-f", file_path])
