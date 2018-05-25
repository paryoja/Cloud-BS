#!/bin/bash

if [ "$#" -eq 0 ] || [ "$1" == "-mv" ]; then
	echo "Input JOB id : (for example, 1471500203111_0006)"
	count=0
	declare -a APP

	while IFS= read -r line; do
		printf '%d %s\n' "$count" "$line"
		APP[count]=`echo $line | cut -f 1 -d \ `
		(( count++ ))
	done < <(yarn application -list -appStates FINISHED,FAILED,KILLED | grep application_ | sort -r)

	read -p "Job number: " number
	echo "Job " ${APP[$number]} " selected"
	JOB=${APP[$number]}
else
	JOB=application_$1
fi

if [ "$1" == "-mv" ] || [ "$2" == "-mv" ]
then
	mv log/*.txt log/old
fi

yarn logs -applicationId $JOB > log/$JOB\_log.txt
if [ $? -ne "0" ]
then
	echo "Error"
	read -p "Press any key to continue..."
fi

