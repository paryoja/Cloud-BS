list=`hdfs dfsadmin -report | grep Hostname: | cut -f 2 -d ' '`

ref_path=hdfs:///user/ubuntu/chr1
local_path=/home/ubuntu/chr_ref_genomes/

for X in $list
do
	echo ssh ubuntu@$X hdfs dfs -get $ref_path $local_path
	ssh-keyscan -H $X 2>/dev/null 1>> ~/.ssh/known_hosts
	ssh ubuntu@$X rm -r $local_path
	ssh ubuntu@$X mkdir $local_path
	ssh ubuntu@$X /home/ubuntu/hadoop/bin/hdfs dfs -get $ref_path/* $local_path
done
