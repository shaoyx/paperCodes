#!/bin/bash
#default queue asynclocal
for((num=1;num<=5;num++))
do
#	./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u.part.60.iter${num} pagewithqueue 16 16 >& .log
#	line=$(cat .log | grep "Running job")
#	jobid=${line#*job:}
#	echo "kill job id=$jobid"
#	echo "$jobid" >> taskconfig1
#	./GiraphJobClean.sh $jobid >& /dev/null

#	./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u.part.60.iter${num} queue 16 16 >& .log
#	line=$(cat .log | grep "Running job")
#	jobid=${line#*job:}
#	echo "kill job id=$jobid"
#	echo "$jobid" >> taskconfig1
#	./GiraphJobClean.sh $jobid >& /dev/null
										
	./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u.part.60.iter${num} asynclocal 16 16 >& .log
	line=$(cat .log | grep "Running job")
	jobid=${line#*job:}
	echo "kill job id=$jobid"
	echo "$jobid" >> taskconfig1
	./GiraphJobClean.sh $jobid >& /dev/null
done
