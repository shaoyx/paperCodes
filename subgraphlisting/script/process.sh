#!/bin/bash

file=$1

hostname=""
superstep=""
curpid=0
processline=0;

while read line
do
	if [ `expr match "$line" "changping"` -gt 0 -a ${#line} -eq 11 ]; then
#		echo $hostname, $curpid, $processline
		if [ "$hostname"  != "" ]; then
			./analysis.sh ${hostname}.${curpid}
			rm ${hostname}.${curpid}
		fi
		hostname=$line;
		((curpid++))
		processline=0;
	elif [ `expr match "$line" "changping"` -gt 0 ]; then
#		echo $hostname, $curpid, $processline
		echo $line | awk '{print $3,$4}' >> ${hostname}.${curpid}
		((processline++));
	fi
done< <(cat $file)

./analysis.sh ${hostname}.${curpid}
rm ${hostname}.${curpid}
