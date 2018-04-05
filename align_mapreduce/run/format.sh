#!/bin/bash
HADOOP_HOME=/usr/local/hadoop
DATA_HOME=/usr/local/hadoop-datastore

stop-history-server.sh
mr-jobhistory-daemon.sh stop historyserver
stop-yarn.sh
stop-dfs.sh

rm -rf $DATA_HOME/hadoop-2.7.3/* $HADOOP_HOME/logs/* /usr/local/spark/logs/*

if [ $# -eq 1 ]
then
	cp $HADOOP_HOME/etc/hadoop/slaves.$1 $HADOOP_HOME/etc/hadoop/slaves
	echo $1 > machine
	#echo $(($1-1)) > machine
fi


for slave in `cat $HADOOP_HOME/etc/hadoop/slaves`
do
   ssh $slave "rm -rf $DATA_HOME/hadoop-2.7.3/* $HADOOP_HOME/logs/* /usr/local/spark/logs/*"&
   ssh $slave "rm -rf /home/hadoop/bio/after_align/* /tmp/map/* "&   
done

wait

for slave in `cat $HADOOP_HOME/etc/hadoop/slaves`
do
   echo $slave 
   scp $HADOOP_HOME/etc/hadoop/slaves $HADOOP_HOME/etc/hadoop/*.xml $HADOOP_HOME/etc/hadoop/*.sh $slave:$HADOOP_HOME/etc/hadoop
   scp /usr/local/spark/conf/spark-defaults.conf /usr/local/spark/conf/spark-env.sh $slave:/usr/local/spark/conf/
done

hdfs namenode -format

start-dfs.sh
sleep 10

hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/hadoop
start-yarn.sh
mr-jobhistory-daemon.sh start historyserver

hdfs dfs -mkdir /user/spark/
hdfs dfs -mkdir /user/spark/applicationHistory
start-history-server.sh
date
