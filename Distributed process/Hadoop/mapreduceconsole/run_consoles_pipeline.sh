#!/bin/bash

# set -e
set -Eeuo pipefail

export KVHOME=/usr/local/kv
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/usr/local/spark
export PATH="$SPARK_HOME/bin:$PATH"
# export SPARK_LOCAL_IP=10.0.2.15
export SPARK_LOCAL_IP=0.0.0.0
export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native";
export LD_LIBRARY_PATH="${HADOOP_COMMON_LIB_NATIVE_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Debug 
command -v spark-submit >/dev/null || { echo "spark-submit introuvable"; exit 127; }

# genete to_create to_udpate to_delete
echo " Étape 1 : Exécution Spark pour extraire les ID a creer, modifier, supprimer"
  $SPARK_HOME/bin/spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
  --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar /vagrant/spark/sync_consoles.py

# To use as input for mapreduce, docs to create and update
echo " Étape 2 : Exécution Spark pour générer docs JSONL"
  $SPARK_HOME/bin/spark-submit \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
  --conf spark.driver.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.executor.extraClassPath=$HOME/kvlib/*:$HOME/datanucleus-fix/*  --conf spark.sql.catalogImplementation=hive  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083  --jars $KVHOME/lib/kvclient.jar,$HOME/datanucleus-fix/datanucleus-core-3.2.10.jar,$HOME/datanucleus-fix/datanucleus-api-jdo-3.2.6.jar,$HOME/datanucleus-fix/datanucleus-rdbms-3.2.9.jar,$HOME/datanucleus-fix/mysql-connector-j-8.0.33.jar /vagrant/spark/sync_consoles2.py

echo " Étape 3 : Supprimer résultat précédent..."
 $HADOOP_HOME/bin/hdfs dfs -rm -r -f resultconsole

echo " Étape 4 : Exécution Hadoop MapReduce streaming"
 $HADOOP_HOME/bin/hadoop jar /usr/local/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -input /tmp/consoles_for_mapreduce.jsonl -output resultconsole -mapper /vagrant/mapreduceconsole/mapper.py -reducer /vagrant/mapreduceconsole/reducer.py

# echo " Étape 5 : Post-traitement"
python3 /vagrant/mapreduceconsole/post_process.py

