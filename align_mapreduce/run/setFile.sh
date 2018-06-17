#!/bin/bash

#hdfs dfs -mkdir data_genomes
#hdfs dfs -put /home/hadoop/bio/data_genomes/sample.filtered.fa data_genomes/sample.filtered.fa

#hdfs dfs -put ~/100.fa fa_data/
hdfs dfs -put ~/CloudAligner/chr1.fa fa_data/
hdfs dfs -put ~/CloudAligner/simulated.fa fa_data/
