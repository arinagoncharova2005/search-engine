#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this
# # remove existing fileas and upload new one
# hdfs dfs -rm -r -f /index/data
# hdfs dfs -mkdir -p /data
hdfs dfs -put -f a.parquet / && \
    spark-submit prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -put -f data / && \
    hdfs dfs -ls /data && \
    echo "dfs -ls /data" && \
    hdfs dfs -ls /index/data && \
    echo "done data preparation!"