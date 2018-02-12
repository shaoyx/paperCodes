#!/bin/bash

if [ $# -ne 2 ]; then
	echo "input: jobid, operationTyper"
fi

for host in `cat hosts`
do
	ssh $host ~/simon/GiraphLogGather.sh $1 $2
done
exit

#######################################################
defaultDir=/home/hadoop/simon/pm

mkdir -p $defaultDir/$1
op=$2

#~/simon/GiraphTaskGather.sh $1
#cp ~/simon/lpm_* $defaultDir/$1/

if [ $op == "Task" ]; then

	for host in `cat hosts`
	do
		ssh $host ~/simon/GiraphTaskGather.sh $1
#		scp $host:/home/hadoop/simon/lpm_$host $defaultDir/$1/ >& /dev/null
	done

fi

if [ $op == "Load" ]; then

	for host in `cat hosts`
	do
		ssh $host ~/simon/GiraphLoadClean.sh $1
	done

fi

if [ $op == "Error" ]; then

	for host in `cat hosts`
	do
		ssh $host ~/simon/GiraphErrorClean.sh $1
	done

fi

#rm ~/simon/lpm_*
#for host in `cat hosts`
#do
#   ssh $host rm ~/simon/lpm_*
#done
