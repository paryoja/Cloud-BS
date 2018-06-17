list=`hdfs dfsadmin -report | grep Hostname: | cut -f 2 -d ' '`

ref_path=hdfs:///test/ref/chr1
local_path=/home/hadoop/bio/chr_ref_genomes/

for X in $list
do
	echo ssh ubuntu@$X hdfs dfs -get $ref_path $local_path
	ssh ubuntu@$X /home/ubuntu/hadoop/bin/hdfs dfs -get $ref_path/* $local_path
done
