#!/bin/bash

set -Eeuo pipefail

export KVHOME=/usr/local/kv
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/usr/local/spark
export PATH="$SPARK_HOME/bin:$PATH"
# export SPARK_LOCAL_IP=10.0.2.15
export SPARK_LOCAL_IP=0.0.0.0
export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native";
export LD_LIBRARY_PATH="${HADOOP_COMMON_LIB_NATIVE_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# NOSQL
echo " Étape 6 : Sync consoleslocal avec consoles"
echo " Étape 6 - 1 : creation data de consoleslocal"
    $SPARK_HOME/bin/spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
  --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar /vagrant/spark/sync_consoles3.py
 $HADOOP_HOME/bin/hdfs dfs -getmerge /tmp/consoles_for_consoleslocal.jsonl /tmp/consoles_for_consoleslocal.jsonl

echo " Étape 6 - 2 : insertion de data de consoleslocal"
java -jar $KVHOME/lib/kvstore.jar runadmin -host localhost -port 5000 <<EOF
connect store -name kvstore;
execute "drop table consoleslocal";
execute "CREATE TABLE consoleslocal  (
  id STRING,
  rev INTEGER,
  PRIMARY KEY(id)
)";
EOF
java -jar $KVHOME/lib/kvstore.jar runadmin -host localhost -port 5000 <<EOF
connect store -name kvstore;
put table -name consoleslocal -file /tmp/consoles_for_consoleslocal.jsonl;
exit;
EOF

# hdfs
echo " Étape 6 - 3 : suppression de documents plus valide"
    $SPARK_HOME/bin/spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
  --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar /vagrant/spark/sync_consoles4.py

echo " Pipeline complet terminé."

