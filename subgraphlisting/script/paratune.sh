#!/bin/bash

if [ $# -lt 1 ];  then
	echo "input: graph type, iternumber[if \"gtype=iter\"]"
	exit
fi

gtype=$1
lconcurrency=1
rconcurrency=1

idx=(1 2 4 8 16 32)
size=6
#try local up concurrency

if [ "$gtype" != "iter" ]; then
	for((i = 0; i < size; i++))
	do
		if [ "$gtype" = "hash" ]; then
#tuning asynclocal
			lconcurrency=${idx[$i]}
			rconcurrency=8
			echo ./test.sh uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 pagewithqueue $lconcurrency $rconcurrency
			./test.sh uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 pagewithqueue $lconcurrency $rconcurrency >& .log
		elif [ "$gtype" = "metis" ]; then
#tuning serverthread
			lconcurrency=16
			rconcurrency=${idx[$i]}
			echo ./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u_n_incoming_outgoing_weights.mgraph.part.60 pagewithqueue $lconcurrency $rconcurrency
			./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u_n_incoming_outgoing_weights.mgraph.part.60 pagewithqueue $lconcurrency $rconcurrency >& .log
		else 
			echo "not supported gtype=$gtype"
			exit
		fi
		#clean job
		line=$(cat .log | grep "Running job")
		jobid=${line#*job:}
		if [ -n "$jobid" ]; then
			echo "gtype=$gtype,local=$lt,remote=$rt ==> $jobid" >> taskconfig
			echo "kill job id=$jobid"
			./GiraphJobClean.sh $jobid >& /dev/null
		fi
	done
elif [ "$gtype" = "iter" ]; then
	num=$2
    total=10
	for((i = 1; i < total; i++))
	do
		lt=$i
		rt=$((total-i))
#		echo $lt, $rt
		echo ./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u.part.60.iter${num} pagewithqueue ${lt} ${rt}
		./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u.part.60.iter${num} pagewithqueue ${lt} ${rt} >& .log
		line=$(cat .log | grep "Running job")
		jobid=${line#*job:}
		echo "kill job id=$jobid"
		if [ -n "$jobid" ]; then
			echo "gtype=$gtype,num=$num,local=$lt,remote=$rt ==> $jobid" >> taskconfig
			echo "kill job id=$jobid"
			./GiraphJobClean.sh $jobid >& /dev/null
		fi
	done
fi

