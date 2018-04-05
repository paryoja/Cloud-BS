MEMORY=2000M
DRIVER_MEMORY=6G
EXECUTOR=20
MASTER=yarn

COAL=$1

OPTION="--master $MASTER --executor-memory $MEMORY --driver-memory $DRIVER_MEMORY --num-executors=$EXECUTOR --conf spark.rpc.message.maxSize=450 --conf spark.network.timeout=10000000 --conf spark.executor.heartbeatInterval=10000000"
echo $OPTION

hdfs dfs -rm -r /user/hadoop/after_align_from_bowtie
date >> from_result.txt
date >> from_time.txt
echo start $MEMORY >> from_result.txt
echo start $MEMORY >> from_time.txt
{ time ~/spark-2.0.2-bin-hadoop2.6/bin/spark-submit $OPTION align_from_bowtie.py $EXECUTOR $COAL &>> from_result.txt ; } &>> from_time.txt
echo end $MEMORY >> from_result.txt
echo end $MEMORY >> from_time.txt
