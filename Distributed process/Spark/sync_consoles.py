
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, regexp_extract, lit

spark = SparkSession.builder.appName("SyncConsoles").enableHiveSupport().getOrCreate()

def norm(df, id_candidates=("id", "doc.id"), rev_candidates=("rev","doc.rev","_rev","doc._rev")):
    # ID : prend le 1er champ non nul parmi id/doc.id
    id_col = coalesce(*[col(c) for c in id_candidates if c in df.columns]).alias("id")
    # REV brut en string (ex: "12-xxxx" ou juste "12")
    rev_raw = coalesce(*[col(c).cast("string") for c in rev_candidates if c in df.columns]).alias("rev_raw")
    # REV numérique = 1er groupe de chiffres en début de chaîne
    rev_num = regexp_extract(col("rev_raw"), r"^(\d+)", 1).cast("int").alias("rev_num")
    return df.select(id_col, rev_raw, rev_num)

# 1) Charger, normaliser, aliasser
remote_src = spark.table("consoles_nosql_ext")
local_src  = spark.table("consoleslocal_nosql_ext")

remote = norm(remote_src).alias("r")
local  = norm(local_src).alias("l")

# 2) Détections
# CREATE: présent dans remote, absent de local
to_create = remote.join(local, on=col("r.id") == col("l.id"), how="left_anti")

# UPDATE: présent des deux côtés ET rev_remote > rev_local
join_rl = remote.join(local, on=col("r.id") == col("l.id"), how="inner")
to_update = join_rl.filter(col("r.rev_num") > col("l.rev_num"))

# DELETE: présent local, absent remote
to_delete = local.join(remote, on=col("l.id") == col("r.id"), how="left_anti")

# 3) Exports
to_create.select(col("r.id").alias("id")).write.mode("overwrite").json("/tmp/consoles/to_create.json")
to_update.select(col("r.id").alias("id")).write.mode("overwrite").json("/tmp/consoles/to_update.json")
to_delete.select(col("l.id").alias("id")).write.mode("overwrite").json("/tmp/consoles/to_delete.json")
