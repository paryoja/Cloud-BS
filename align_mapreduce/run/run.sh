#!/bin/bash

#./git.sh

#hdfs dfs -rm -r /user/hadoop/temp
#hdfs dfs -rm -r /home/hadoop/mapred/output  /user/hadoop/mapred 
#hdfs dfs -rm -r /user/hadoop/temp 

#time yarn jar align.jar Align -ifile sample/sample -ofile mapred/output -rfile /home/hadoop/bio/chr_ref_genomes/chr1.fa -machines 2
#time yarn jar align.jar Align -ifile small/small.fa -ofile mapred/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -machines 20
#time yarn jar align.jar Align -ifile biodata/srr_10m.fa -ofile mapred/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -machines 20
#time yarn jar align.jar Align -ifile biodata/SRR_1.fa -ofile mapred/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -machines 20
#time yarn jar align.jar Align -ifile biodata/SRR306438_1.fasta -ofile mapred/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -machines 20

#time yarn jar align.jar Align -ifile biodata/humandata_100m.fa -ofile mapred/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -machines 20
#time yarn jar align.jar Align -ifile big/humandata.fa -ofile mapred/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -machines 20


#hdfs dfs -rm -r bypass_mr
#time yarn jar align.jar WithoutBowtie -ifile biodata/humandata_100m.fa -ofile bypass_mr/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -bypass False -machines 20



#hdfs dfs -rm -r /home/hadoop/mapred/output  /user/hadoop/mapred 
#hdfs dfs -rm -r /user/hadoop/temp
#hdfs dfs -rm -r bypass_mr
#./clear_emit.sh
#time yarn jar align.jar WithoutBowtie -ifile biodata/humandata_100m.fa -ofile bypass_mr/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -bypass True -machines 20
time yarn jar align.jar Align -ifile fa_data/100.fa -ofile output -rfile fa_data/chr1.fa -machines 20

#hdfs dfs -rm -r from/output
#time yarn jar align.jar From -ifile bowtie/output  -ofile from/output -rfile /home/hadoop/bio/chr_ref_genomes/splitted -machines 20
