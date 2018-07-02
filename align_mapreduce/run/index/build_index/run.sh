#!/bin/bash

hdfs dfs -put ~/chr1.fa /user/ubuntu/fa_data/

input="hdfs:///user/ubuntu/fa_data/chr1.fa"
output="hdfs:///user/ubuntu/chr1"
log="/home/ubuntu/log.txt"


hdfs dfs -rm -r -f $output


basepath=$(dirname $0)



python "${basepath}/build_index.py" \
  --input $input \
  --output $output \
  --log $log

hdfs dfs -cp $input $output
indexDir=$output"/index/*"
hdfs dfs -mv $indexDir $output
