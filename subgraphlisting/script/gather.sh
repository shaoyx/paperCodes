#!/bin/bash
if [ $# -ne 2 ]; then
	echo " jobid pmName"
fi
hadoop fs -rm /user/simon/uk-2007-05-u/partitionmap/$2
rm tmp
./GiraphGather.sh $1 PM >> tmp
cat tmp | grep -v "log" > $2
hadoop fs -put $2 /user/simon/uk-2007-05-u/partitionmap/$2
