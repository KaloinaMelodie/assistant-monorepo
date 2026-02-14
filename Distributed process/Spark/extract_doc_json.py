from pyspark.sql import SparkSession
import json

spark = SparkSession.builder \
    .appName("ExportDocumentsJSON") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("""
    SELECT 
        id, 
        get_json_object(doc, '$.name') as nom,
        get_json_object(doc, '$.extension') as extension
    FROM documents_nosql_ext
""")

data = df.toJSON().map(lambda x: json.loads(x)).collect()

output_path = "/vagrant/documents_export.json"

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"Export JSON terminé dans : {output_path}")
