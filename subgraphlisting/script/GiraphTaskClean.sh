#!/bin/bash
job=$1
taskSig=${job#job_}
attempTaskPrefix=attempt_$taskSig
#echo $attempTaskPrefix

for((i=1;$i;i++))
do
	relatedProcess=$(ps -ef | grep $attempTaskPrefix | grep java)

	#echo $relatedProcess
	strLen=${#relatedProcess}
	echo "realtedProcess Length $strLen"

	if [ $strLen -gt 100 ]; then
		pid=$(echo $relatedProcess | awk '{print $2}')
		echo "Kill $pid"
		echo "$relatedProcess"
		kill -9 $pid
	else
		break;
	fi
done
