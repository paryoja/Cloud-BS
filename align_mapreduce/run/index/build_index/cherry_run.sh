#!/bin/bash

#input="hdfs:////test/raw/chr1.fa"
input="hdfs:///user/hadoop/fa_data/chr1.fa"
#output="hdfs:///test/ref/chr1"
output="hdfs:///user/hadoop/ref/chr1"
log="./log.txt"


#hdfs dfs -rm -r -f $output


basepath=$(dirname $0)



python "${basepath}/build_index.py" \
  --input $input \
  --output $output \
  --log $log
