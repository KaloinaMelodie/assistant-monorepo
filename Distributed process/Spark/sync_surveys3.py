from pyspark.sql import SparkSession, functions as F

OUT_PATH = "hdfs://localhost:9000/tmp/surveys_for_surveyslocal.jsonl"  

spark = (SparkSession.builder
         .appName("ExtractFieldsForSurveysLocal")
         .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
         .enableHiveSupport()
         .getOrCreate())


df = spark.table("surveys_nosql_ext")

cols = set(df.columns)

id_exprs  = [F.col(c) for c in ("_id", "id") if c in cols]
rev_exprs = [F.col(c) for c in ("_rev", "rev") if c in cols]

if "doc" in cols:
    doc_field = df.schema["doc"]
    if doc_field.dataType.simpleString().startswith("struct"):
        if "_id" in doc_field.dataType.fieldNames(): 
            id_exprs.append(F.col("doc._id"))
        if "id" in doc_field.dataType.fieldNames():   
            id_exprs.append(F.col("doc.id"))
        if "_rev" in doc_field.dataType.fieldNames(): 
            rev_exprs.append(F.col("doc._rev"))
        if "rev" in doc_field.dataType.fieldNames():  
            rev_exprs.append(F.col("doc.rev"))
    else:
        id_exprs.append(F.get_json_object(F.col("doc"), "$._id"))
        id_exprs.append(F.get_json_object(F.col("doc"), "$.id"))
        rev_exprs.append(F.get_json_object(F.col("doc"), "$._rev"))
        rev_exprs.append(F.get_json_object(F.col("doc"), "$.rev"))

id_col  = F.coalesce(*id_exprs)  if id_exprs  else F.lit(None)
rev_col = F.coalesce(*rev_exprs) if rev_exprs else F.lit(None)

out = df.select(
    F.trim(id_col).alias("id"),
    rev_col.cast("int").alias("rev") 
)

(out
    .write.mode("overwrite").json(OUT_PATH))

spark.stop()
