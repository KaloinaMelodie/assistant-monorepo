import site
site.addsitedir('/home/vagrant/.local/lib/python3.9/site-packages')
from pyspark.sql import SparkSession
import requests
import json

# === CONFIGURATION ===
API_URL = "http://localhost:8000/surveys/create"  # change si autre port/host

# === Spark Session ===
spark = SparkSession.builder \
    .appName("SendCreateToFastAPI") \
    .enableHiveSupport() \
    .getOrCreate()

# === Lire la vue Hive ===
df = spark.table("surveys_with_content")

# === Fonction pour nettoyer les champs liste (format string : '["a","b"]') ===
def parse_list_string(s):
    try:
        return json.loads(s) if s else []
    except Exception:
        return []

# === Envoyer chaque ligne à l’API ===
for row in df.collect():
    data = {
        "id": row["id"],
        "rev": row["rev"],
        "nom": row["nom"],
        "langue": row["langue"],
        "emplacement": parse_list_string(row["emplacement"]),
        "accessright": parse_list_string(row["accessright"]),
        "content": row["content"]
    }

    try:
        response = requests.post(API_URL, json=data)
        print(f"[{row['id']}] → {response.status_code} : {response.text}")
    except Exception as e:
        print(f"❌ Erreur pour {row['id']} : {e}")
