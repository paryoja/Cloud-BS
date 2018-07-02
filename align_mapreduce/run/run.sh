#!/bin/bash

#./git.sh

OUTPUT_PATH=output
INPUT_DIR=fa_data
INPUT_FILE=simulated.fa
REFERENCE_DIR=/home/ubuntu/chr_ref_genomes/
RESULT_FILE=alignmentResult_${INPUT_FILE/.fa/.sam}
NUM_NODES=3

#echo $RESULT_FILE
#echo $INPUT_DIR/$INPUT_FILE

hdfs dfs -rm -r $OUTPUT_PATH
hdfs dfs -rm -r /user/ubuntu/temp


time yarn jar align.jar Align -ifile $INPUT_DIR/$INPUT_FILE -ofile $OUTPUT_PATH -rfile $REFERENCE_DIR -machines $NUM_NODES
hdfs dfs -cat $OUTPUT_PATH/part-r-* > $RESULT_FILE

