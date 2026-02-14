#!/bin/bash

set -Eeuo pipefail

export KVHOME=/usr/local/kv
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/usr/local/spark
export PATH="$SPARK_HOME/bin:$PATH"
export SPARK_LOCAL_IP=0.0.0.0
export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native";
export LD_LIBRARY_PATH="${HADOOP_COMMON_LIB_NATIVE_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
# export PYSPARK_PYTHON=/usr/bin/python3
# export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

command -v spark-submit >/dev/null || { echo "spark-submit introuvable"; exit 127; }

# genete to_create to_udpate to_delete
echo " Étape 1 : Exécution Spark pour extraire les ID a creer, modifier, supprimer"
  $SPARK_HOME/bin/spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
  --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar /vagrant/spark/sync_documents.py


# To use as input for download files, docs to create and update
echo " Étape 2 : Exécution Spark pour générer docs JSONL"
  $SPARK_HOME/bin/spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
  --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar /vagrant/spark/sync_documents2.py

echo " Étape 3 : Téléchargement des documents depuis l'API"
  $SPARK_HOME/bin/spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
  --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar /vagrant/spark/sync_documents3.py


# echo "Étape 4 : Extraction de list de documents JSON""
# $SPARK_HOME/bin/spark-submit \
#   --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar /vagrant/spark/extract_doc_json.py

echo " Étape 4 : Transformation des fichiers en fichier .txt"
  $SPARK_HOME/bin/spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
  --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar \
  --conf spark.sql.sources.ignoreCorruptFiles=true \
  /vagrant/spark/sync_documents4.py \
   --in hdfs:///user/vagrant/docs_downloaded_hdfs \
   --out hdfs:///user/vagrant/output_docs_document_hdfs \
   --tmp hdfs:///user/vagrant/output_docs_document_hdfs_tmp \
   --sample_rows 300 --min_chars 200 --enable_ocr false --tesseract_cmd /usr/bin/tesseract 
  # # --conf spark.executorEnv.PYSPARK_PYTHON="$PYSPARK_PYTHON" \
  # # --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="$PYSPARK_PYTHON" \