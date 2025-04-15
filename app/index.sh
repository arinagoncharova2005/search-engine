#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input file is:"
echo $1


hdfs dfs -ls /

# set input path as the first argument if it is passed
INPUT_PATH=$1
if [ -z "$INPUT_PATH" ]; then
  echo "No local file path is given."
  INPUT_PATH="/index/data"
else
    hdfs dfs -mkdir -p /index/data
    echo "Copy given local file to HDFS"
    if [ -d "$INPUT_PATH" ]; then
        hdfs dfs -put $INPUT_PATH/* /index/data/
    else
        hdfs dfs -put $INPUT_PATH /index/data/
    fi
fi


OUTPUT_DIR='/tmp/index/output'

echo Current working directory:
echo $PWD
hdfs dfs -rm -r -f $OUTPUT_DIR
# echo 'Create temp input and output directories in hdfs'
# hdfs dfs -mkdir -p $OUTPUT_DIR

echo 'Add permissions to .py files'
chmod +x $PWD/mapreduce/mapper1.py
chmod +x $PWD/mapreduce/reducer1.py

source .venv/bin/activate
echo 'Start mapreduce'

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -archives /app/.venv.tar.gz#.venv \
    -mapper ".venv/bin/python mapper1.py" \
    -reducer ".venv/bin/python reducer1.py" \
    -input $INPUT_PATH \
    -output $OUTPUT_DIR

echo 'Mapreduce job was finished successfully!'
