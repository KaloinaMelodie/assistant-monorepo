#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
make_ndjson_page.py
-------------------
Cas d'entrée: un fichier d’UNE SEULE LIGNE contenant des objets JSON collés
(sans [] ni ,), p.ex.  {"a":1}{"a":2}{"a":3}

Pipeline:
- Lecture en TEXTE (FS local)
- Insertion de "\n" entre les jonctions '}{' (regex Java: \}\s*\{ )
- split -> explode -> 1 chaîne JSON par objet
- Inférence d'un schéma sur le 1er objet
- from_json -> DataFrame structuré
- Écriture NDJSON en **UN SEUL FICHIER** (jamais un dossier au chemin --output)

Exemple:
  spark-submit make_ndjson_page.py \
    --input  "file:///vagrant/page/raw_pages.jsonl" \
    --output "file:///vagrant/page/pages.ndjson" \
    --coalesce 1
"""

import argparse
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    p = argparse.ArgumentParser(description="Concat-JSON (1 ligne) -> NDJSON via PySpark.")
    p.add_argument("--input", required=True,
                   help="Chemin d'entrée (recommandé: file:///... pour forcer FS local).")
    p.add_argument("--output", required=True,
                   help="Chemin de sortie NDJSON **fichier** (file:///... ou chemin POSIX).")
    p.add_argument("--coalesce", type=int, default=1,
                   help="Partitions sortie (défaut: 1).")
    return p.parse_args()


def build_spark():
    spark = (
        SparkSession.builder
        .appName("fix-concat-json-to-ndjson")
        .config("spark.hadoop.fs.defaultFS", "file:///")  # éviter HDFS
        # option: ne pas écrire _SUCCESS (pas indispensable)
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _normalize_output_path(output_path_str: str) -> Path:
    """Transforme file:///... en chemin local Path()."""
    if output_path_str.startswith("file:///"):
        return Path(output_path_str[len("file://"):])
    return Path(output_path_str)


def write_single_file_text(df_with_json_col, output_path_str: str, coalesce: int = 1):
    """
    Écrit df (colonne 'json') comme NDJSON **fichier unique** au chemin --output.
    - Écrit d'abord dans un répertoire temporaire (format text)
    - Récupère le part-*
    - Si --output existe et est un **dossier**, le supprime (pour éviter IsADirectoryError)
    - Si --output existe et est un **fichier**, le supprime
    - Déplace part-* -> --output (fichier)
    """
    output_path = _normalize_output_path(output_path_str)

    # 1) Écriture dans un dossier temporaire
    tmpdir = Path(tempfile.mkdtemp(prefix="spark_text_out_"))
    try:
        (
            df_with_json_col.coalesce(max(1, coalesce))
            .write
            .mode("overwrite")
            .text(str(tmpdir))
        )

        # 2) Trouver le part-*
        parts = list(tmpdir.glob("part-*"))
        if not parts:
            raise SystemExit("[ERROR] Aucune sortie générée (part-* introuvable).")
        part = parts[0]

        # 3) Nettoyer un éventuel chemin existant (dossier ou fichier)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            if output_path.is_dir():
                shutil.rmtree(output_path)  # << clé: si un run précédent a créé un dossier
            else:
                output_path.unlink()

        # 4) Déplacer/renommer le part-* vers le chemin final (fichier)
        shutil.move(str(part), str(output_path))

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    args = parse_args()
    spark = build_spark()

    # 1) Lire comme TEXTE (FS local)
    df_txt = spark.read.text(args.input)
    if df_txt.rdd.isEmpty():
        raise SystemExit("[ERROR] Entrée vide. Vérifie --input.")

    # 2) Insérer des sauts de ligne entre '}{' (accollades échappées en Java regex)
    with_breaks = df_txt.select(
        F.regexp_replace(F.col("value"), r"\}\s*\{", "}\n{").alias("value")
    )

    # 3) split -> explode pour obtenir une ligne JSON par objet
    splitted = with_breaks.select(F.split(F.col("value"), r"\n").alias("lines"))
    exploded = splitted.select(F.explode(F.col("lines")).alias("json_str"))
    cleaned = exploded.select(F.trim(F.col("json_str")).alias("json_str")).filter(F.col("json_str") != "")

    # 4) Inférer le schéma à partir du premier objet
    first = cleaned.select("json_str").limit(1).collect()
    if not first:
        raise SystemExit("[ERROR] Aucun objet détecté après découpe. Vérifie le format d'entrée.")
    sample_json = first[0]["json_str"]
    schema = spark.range(1).select(
        F.schema_of_json(F.lit(sample_json)).alias("schema")
    ).collect()[0]["schema"]

    # 5) Parser toutes les lignes avec le schéma inféré
    parsed = cleaned.select(F.from_json(F.col("json_str"), schema).alias("_obj")).select("_obj.*")

    # 6) Écrire en NDJSON **fichier unique**
    json_lines = parsed.select(F.to_json(F.struct(*[F.col(c) for c in parsed.columns])).alias("json"))
    write_single_file_text(json_lines.select("json"), args.output, coalesce=args.coalesce)

    spark.stop()
    print(f"[OK] NDJSON écrit dans : {args.output}")


if __name__ == "__main__":
    main()
