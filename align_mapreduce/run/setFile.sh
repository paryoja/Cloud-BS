#!/bin/bash

hdfs dfs -mkdir data_genomes
hdfs dfs -put /home/hadoop/bio/data_genomes/sample.filtered.fa data_genomes/sample.filtered.fa
