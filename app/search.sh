#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

# path for the results
OUTPUT_PATH="/tmp/index/top_10"

# remove the previous result
echo "Deleting previous search results"
hdfs dfs -rm -r -f $OUTPUT_PATH

spark-submit \
  --master yarn \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
  --archives /app/.venv.tar.gz#.venv \
  query.py "$1"