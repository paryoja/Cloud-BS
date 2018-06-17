#!/bin/bash
hdfs dfs -mkdir /test
hdfs dfs -mkdir /test/ref
hdfs dfs -mkdir /test/raw
hdfs dfs -put ~/chr1.fa /test/raw/

input="hdfs:////test/raw/chr1.fa"
output="hdfs:///test/ref/chr1"
log="~/build_log.txt"


hdfs dfs -rm -r -f $output


basepath=$(dirname $0)



python "${basepath}/build_index.py" \
  --input $input \
  --output $output \
  --log $log
