from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField

# 1. Initialiser Spark avec Hive support
spark = SparkSession.builder \
    .appName("SyncConsoles2") \
    .enableHiveSupport() \
    .getOrCreate()

    # Schéma d’un DataFrame contenant une colonne "id"
id_schema = StructType().add("id", StringType())

# Lire les deux fichiers avec le bon schéma
to_create_df = spark.read.schema(id_schema).json("/tmp/consoles/to_create.json")
to_update_df = spark.read.schema(id_schema).json("/tmp/consoles/to_update.json")

# S’assurer que les deux DF ont bien la colonne "id"
to_create_df = to_create_df.select("id")
to_update_df = to_update_df.select("id")

# Fusionner sans erreur
all_ids = to_create_df.union(to_update_df).distinct()

# 3. Recharger la table consoles et parser le champ 'doc'
consoles = spark.table("consoles_nosql_ext")

# 4. Join avec la liste d'ID pour récupérer les documents concernés
docs_to_process = all_ids.join(consoles, on="id", how="inner").select("doc")

# 5. Sauvegarde en NDJSON (JSONL)
docs_to_process.write.mode("overwrite").text("/tmp/consoles_for_mapreduce.jsonl")
