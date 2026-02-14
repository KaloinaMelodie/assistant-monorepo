from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

# 1) Spark + Hive
spark = SparkSession.builder \
    .appName("SyncDocuments") \
    .enableHiveSupport() \
    .getOrCreate()

# 2) Charger les tables (distant vs local)
remote = spark.table("documents_nosql_ext") \
    .select(
        col("id"),
        col("rev").alias("rev_remote")
    ) \
    .withColumn("rev_remote_num", split(col("rev_remote"), "-").getItem(0).cast("int")) \
    .alias("r")

local = spark.table("documentslocal_nosql_ext") \
    .select(
        col("id"),
        col("rev").alias("rev_local")
    ) \
    .withColumn("rev_local_num", split(col("rev_local"), "-").getItem(0).cast("int")) \
    .alias("l")

# 3) Détections (left_anti = "dans A, pas dans B")
# CREATE: dans remote, pas dans local
to_create = remote.join(local, on="id", how="left_anti")

# UPDATE: id présent des deux côtés ET rev_remote > rev_local
to_update = remote.join(local, on="id", how="inner") \
    .filter(col("r.rev_remote_num") > col("l.rev_local_num"))

# DELETE: dans local, pas dans remote
to_delete = local.join(remote, on="id", how="left_anti")

# 4) Exports (adapte les chemins si besoin)
to_create.select("id").write.mode("overwrite").json("/tmp/documents/to_create.json")
to_update.select("id").write.mode("overwrite").json("/tmp/documents/to_update.json")
to_delete.select("id").write.mode("overwrite").json("/tmp/documents/to_delete.json")
