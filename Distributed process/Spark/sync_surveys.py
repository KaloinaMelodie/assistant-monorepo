from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField

# 1. Initialiser Spark avec Hive support
spark = SparkSession.builder \
    .appName("SyncSurveys") \
    .enableHiveSupport() \
    .getOrCreate()

hc = spark.sparkContext._jsc.hadoopConfiguration()
hc.set("oracle.kv.requestTimeout", "30000")       
hc.set("oracle.kv.socketReadTimeout", "60000")    
hc.set("oracle.kv.parallelScan.openTimeout", "60000")

# 2. Définir le schéma JSON du champ 'doc'
doc_schema = StructType([
    StructField("state", StringType(), True)
])

# 3. Charger les tables Hive
surveys = spark.table("surveys_nosql_ext")
surveyslocal = spark.table("surveyslocal_nosql_ext")

# 4. Parser 'doc' en STRUCT et extraire 'state'
surveys = surveys \
    .withColumn("doc_parsed", from_json(col("doc"), doc_schema)) \
    .withColumn("state", col("doc_parsed.state"))

# 5. Détection des entrées actives
active = surveys.filter(col("state") == "active").alias("s")
surveyslocal = surveyslocal.alias("l")

# 6. Jointure pour détection de création ou mise à jour
joined = active.join(surveyslocal, "id", "left_outer")

to_create = joined.filter(col("l.id").isNull())
to_update = joined.filter(
    col("l.id").isNotNull() & (col("s.rev") > col("l.rev"))
)

# 7. Jointure pour détection des suppressions
joined_del = surveyslocal.alias("l").join(surveys.alias("s"), "id", "left_outer")
to_delete = joined_del.filter(
    col("s.id").isNull() | (col("s.state") == "deleted")
)

# 8. Exporter les résultats
to_create.select("id").write.mode("overwrite").json("/tmp/to_create.json")
to_update.select("id").write.mode("overwrite").json("/tmp/to_update.json")
to_delete.select("id").write.mode("overwrite").json("/tmp/to_delete.json")
