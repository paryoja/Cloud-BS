#!/bin/bash

mapred job -list 2>/dev/null | while read line 2>/dev/null
do
	if [[ $line == job* ]]
	then
		jobId=`echo $line | cut -d ' ' -f 1`
		echo 'Killing ' $jobId
		mapred job -kill $jobId 2>/dev/null
	fi
done

echo 'List job'
mapred job -list 2>/dev/null
