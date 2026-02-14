from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql.types import StructType, StringType

# 1) Spark + Hive
spark = SparkSession.builder \
    .appName("ExportDocumentsJSON_Filtered") \
    .enableHiveSupport() \
    .getOrCreate()

# 2) Schéma des fichiers d'IDs
id_schema = StructType().add("id", StringType())

# 3) Charger les IDs à créer et à mettre à jour (JSON lines)
to_create_df = spark.read.schema(id_schema).json("/tmp/documents/to_create.json").select("id")
to_update_df = spark.read.schema(id_schema).json("/tmp/documents/to_update.json").select("id")

# 4) Union + distinct pour obtenir l’ensemble des IDs
all_ids = to_create_df.union(to_update_df).distinct()

# 5) Table NoSQL externe
docs = spark.table("documents_nosql_ext")

# 6) Filtrer par inner join sur id
filtered = docs.join(all_ids, on="id", how="inner") \
    .select(
        col("id"),
        get_json_object(col("doc"), "$.name").alias("nom"),
        get_json_object(col("doc"), "$.extension").alias("extension")
    )

output_dir = "hdfs:///tmp/documents_for_download.jsonl"
filtered.write.mode("overwrite").json(output_dir)

# (Optionnel) Si un seul fichier, décommente la ligne suivante
# filtered.coalesce(1).write.mode("overwrite").json(output_dir + "_single")

print(f"Export JSON terminé vers le dossier HDFS : {output_dir}")
