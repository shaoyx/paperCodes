#!/bin/bash
#for job in `awk '{print $3}' taskconfig`

rm ljtaskconfig

#while read line 
for line in `cat ljpm`
do
	echo $line
	pn=$(echo $line | awk -F "#" '{print $1}')
	pm=$(echo $line | awk -F "#" '{print $2}')
	echo ./test.id soc-LiveJournal SimplePageRankVertex adjformate $pn $pm default 16 16
	./test.id soc-LiveJournal SimplePageRankVertex adjformate $pn $pm asynclocal 16 16 >& .log
	line=$(cat .log | grep "Running job")
	jobid=${line#*job:}
	if [ -n "$jobid" ]; then
		echo "kill job id=$jobid"
		echo "pn=$pn,pm=$pm ==> $jobid" >> ljtaskconfig
		./GiraphJobClean.sh $jobid >& /dev/null
		./GiraphGather.sh $jobid Task >> ${jobid}.log
		./GiraphGather.sh $jobid Load >> ${jobid}.log
	fi

#	echo ./test.id soc-LiveJournal SimplePageRankVertex adjformate $pn $pm pagewithqueue 16 16
#	./test.id soc-LiveJournal SimplePageRankVertex adjformate $pn $pm pagewithqueue 16 16 >& .log
#	line=$(cat .log | grep "Running job")
#	jobid=${line#*job:}
#	if [ -n "$jobid" ]; then
#		echo "kill job id=$jobid"
#		echo "pn=$pn,pm=$pm ==> $jobid" >> ljtaskconfig
#		./GiraphJobClean.sh $jobid >& /dev/null
#		./GiraphGather.sh $jobid Task >> ${jobid}.log
#		./GiraphGather.sh $jobid Load >> ${jobid}.log
#	fi
done
exit

#for((i=2;i<=20;i++))
#do
#	./paratune.sh iter $i
#done


#./GiraphGather.sh job_201304171640_0596 Task >> asynclocal.metis.part60.log596
#./GiraphGather.sh job_201304171640_0596 Load >> asynclocal.metis.part60.log596
#./extract.sh asynclocal.metis.part60.log596

#./GiraphGather.sh job_201304171640_0605 Task >> pagewiththread.metis.part60.log605
#./GiraphGather.sh job_201304171640_0605 Load >> pagewiththread.metis.part60.log605
#./extract.sh pagewiththread.metis.part60.log605

#./GiraphGather.sh job_201304171640_0607 Task >> queuethread.metis.part60.log607
#./GiraphGather.sh job_201304171640_0607 Load >> queuethread.metis.part60.log607
#./extract.sh queuethread.metis.part60.log607
#./GiraphGather.sh job_201304171640_0609 Task >> queue.hash.part60.log609
#./GiraphGather.sh job_201304171640_0609 Load >> queue.hash.part60.log609
#./extract.sh queue.hash.part60.log609

#./GiraphGather.sh job_201304171640_0612 Task >> pagewiththread.hash.part60.log612
#./GiraphGather.sh job_201304171640_0612 Load >> pagewiththread.hash.part60.log612
#./extract.sh pagewiththread.hash.part60.log612

#./GiraphGather.sh job_201304171640_0613 Task >> queuethread.hash.part60.log613
#./GiraphGather.sh job_201304171640_0613 Load >> queuethread.hash.part60.log613
#./extract.sh queuethread.hash.part60.log613

exit
iterNumber=1
jobid=83

for((; iterNumber<=50;iterNumber++))
do
	((jobid++));
	echo ./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u.part.60.iter${iterNumber} default 1
	./test.id uk-2007-05-u SimplePageRankVertex uk-2007-05-u-n.txt 60 uk_2007_05_u.part.60.iter${iterNumber} default 1
	if [ $jobid -lt 100 ]; then
		echo "./GiraphGather.sh job_201304171640_00$jobid Task >> default.part60.iter${iterNumber}.log${jobid}"
		./GiraphGather.sh job_201304171640_00$jobid Task >> default.part60.iter${iterNumber}.log${jobid}
		echo "./GiraphGather.sh job_201304171640_00$jobid Load >> default.part60.iter${iterNumber}.log${jobid}"
		./GiraphGather.sh job_201304171640_00$jobid Load >> default.part60.iter${iterNumber}.log${jobid}
		echo "./GiraphJobClean.sh job_201304171640_00$jobid"
		./GiraphJobClean.sh job_201304171640_00$jobid
	else
		echo "./GiraphGather.sh job_201304171640_0$jobid Task >> default.part60.iter${iterNumber}.log${jobid}"
		./GiraphGather.sh job_201304171640_0$jobid Task >> default.part60.iter${iterNumber}.log${jobid}
		echo "./GiraphGather.sh job_201304171640_0$jobid Load >> default.part60.iter${iterNumber}.log${jobid}"
		./GiraphGather.sh job_201304171640_0$jobid Load >> default.part60.iter${iterNumber}.log${jobid}
		echo "./GiraphJobClean.sh job_201304171640_0$jobid"
		./GiraphJobClean.sh job_201304171640_0$jobid
	fi
done
