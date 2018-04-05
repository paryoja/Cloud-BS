MEMORY=2500M
DRIVER_MEMORY=6G
EXECUTOR=20
FILENAME=$1
BYPASS=$2

OPTION="--master yarn --executor-memory $MEMORY --driver-memory $DRIVER_MEMORY --num-executors=$EXECUTOR --conf "spark.rpc.message.maxSize=450" --conf spark.network.timeout=10000000 --conf spark.executor.heartbeatInterval=10000000"
echo $OPTION

hdfs dfs -rm -r /user/hadoop/bypass
#hdfs dfs -rm -r /user/hadoop/after_align/ 
date >> result.txt
date >> time.txt
echo start $MEMORY >> result.txt
echo start $MEMORY >> time.txt
{ time ~/spark-2.0.2-bin-hadoop2.6/bin/spark-submit $OPTION bypass_partition.py hdfs $FILENAME $BYPASS &>> result.txt ; } &>> time.txt
echo end $MEMORY >> result.txt
echo end $MEMORY >> time.txt


