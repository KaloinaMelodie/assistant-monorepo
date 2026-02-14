#!/usr/bin/env python3
import os
import subprocess
import shutil
import re
from pathlib import Path
import errno
import time


HDFS_RESULT_DIR = "resultdoc"
SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_MERGED_FILE = SCRIPT_DIR /  "merged_output.txt"
LOCAL_OUTPUT_DIR = SCRIPT_DIR / "output_docs"
HDFS_DEST_DIR = "output_docs_hdfs"
HDFS_BIN = "/usr/local/hadoop/bin/hdfs"

def fetch_and_merge_hdfs_parts(hdfs_dir, local_file):
    print(f" Fusion des fichiers de {hdfs_dir} depuis HDFS...")
    subprocess.run([HDFS_BIN, "dfs", "-getmerge", hdfs_dir, local_file], check=True)

def create_files_from_merged_file(local_file, output_dir):
    """Crée des fichiers .txt individuels par _id, avec ponctuation finale si besoin.
       Gère soit 'id\tidx\ttext', soit 'id\ttext'."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    files = {}
    with open(local_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            # Essayer d'abord format reducer: id\ttext
            parts = line.split("\t", 2)
            if len(parts) == 3:
                # mapper brut: id\tidx\ttext
                doc_id, maybe_idx, text = parts
                # si maybe_idx est un int → on garde text
                try:
                    _ = int(maybe_idx)
                    pass
                except ValueError:
                    # en réalité c'était déjà reducer: id\t(text with tab?) — fallback
                    doc_id = parts[0]
                    text = parts[1] + ("\t" + parts[2] if len(parts) > 2 else "")
            elif len(parts) == 2:
                # reducer: id\ttext
                doc_id, text = parts
            else:
                continue

            files.setdefault(doc_id, []).append(text)

    def should_skip_dot(line: str) -> bool:
        """Ne pas ajouter de point si ligne spéciale ou titre."""
        s = line.strip()
        if not s:
            return True
        # Lignes "Legendes : ...", "Type de visuels : ..."
        if s.startswith("Legendes :") or s.startswith("Légendes :") or s.startswith("Type de visuels :"):
            return True
        # Titres/étiquettes finissant par ':'
        if s.endswith(":"):
            return True
        return False

    def ensure_terminal_punctuation(sentence: str) -> str:
        s = sentence.strip()
        if not s:
            return s
        if should_skip_dot(s):
            return s
        # if already ends with strong punctuation, keep
        if re.search(r'[.!?…]$', s):
            return s
        return s + "."

    for doc_id, lines in files.items():
        cleaned = [ensure_terminal_punctuation(line) for line in lines]
        with open(os.path.join(output_dir, f"{doc_id}.txt"), "w", encoding="utf-8") as f:
            f.write(" ".join(cleaned).replace("\n", " "))

    print(f" Fichiers générés localement dans : {output_dir}")

def upload_to_hdfs(local_dir, hdfs_dir):
    print(f" Transfert vers HDFS : {hdfs_dir}")
    subprocess.run([HDFS_BIN, "dfs", "-mkdir", "-p", hdfs_dir], check=True)

    for file_name in os.listdir(local_dir):
        local_path = os.path.join(local_dir, file_name)
        if os.path.isfile(local_path) and file_name.endswith(".txt"):
            subprocess.run(
                [HDFS_BIN, "dfs", "-put", "-f", local_path, os.path.join(hdfs_dir, file_name)],
                check=True
            )
    print("Fichiers mis à jour dans HDFS sans suppression globale.")

# def cleanup_local(local_files):
#     print("Nettoyage des fichiers locaux...")
#     for path in local_files:
#         if os.path.isdir(path):
#             shutil.rmtree(path)
#         elif os.path.isfile(path):
#             os.remove(path)
#     print(" Nettoyage terminé.")



def _safe_remove_tree_contents(dir_path, retries=5, delay=0.2):
    for attempt in range(retries):
        busy = False
        with os.scandir(dir_path) as it:
            for entry in it:
                p = entry.path
                try:
                    if entry.is_dir(follow_symlinks=False):
                        shutil.rmtree(p)
                    else:
                        os.unlink(p)
                except OSError as e:
                    # EBUSY (26) ou "Text file busy" -> on retente
                    if e.errno in (errno.EBUSY, ) or "Text file busy" in str(e):
                        busy = True
                        continue
                    # ENOENT si le fichier a disparu entre temps -> ignorer
                    if e.errno == errno.ENOENT:
                        continue
                    raise
        if not busy:
            return
        time.sleep(delay * (2 ** attempt))  # backoff exponentiel

def cleanup_local(local_paths):
    print("Nettoyage des fichiers locaux...")
    for path in local_paths:
        if os.path.isdir(path):
            # 1) On vide le dossier (plus sûr sur /vagrant)
            try:
                _safe_remove_tree_contents(path)
            except FileNotFoundError:
                pass  # déjà parti
        else:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
    print(" Nettoyage terminé.")



if __name__ == "__main__":
    fetch_and_merge_hdfs_parts(HDFS_RESULT_DIR, LOCAL_MERGED_FILE)
    create_files_from_merged_file(LOCAL_MERGED_FILE, LOCAL_OUTPUT_DIR)
    upload_to_hdfs(LOCAL_OUTPUT_DIR, HDFS_DEST_DIR)
    cleanup_local([LOCAL_MERGED_FILE, LOCAL_OUTPUT_DIR])
