#!/bin/bash
HADOOP_HOME=/usr/local/hadoop
DATA_HOME=/usr/local/hadoop-datastore


for slave in `cat slaves`
do
   ssh $slave "sudo rm -rf /tmp/bypass/*.emit"&
done

wait
