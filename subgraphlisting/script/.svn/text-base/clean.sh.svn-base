#!/bin/bash

cmd=$1
logfile=$2
echo $cmd
echo $logfile

line=$(cat $logfile | grep "Running job")
jobid=${line#*job:}
	
if [ -n "$jobid" ]; then
    echo "kill job id=$jobid"
    echo "$cmd ==> $jobid" >> jobcmd.record
    ssh crazygraph@localhost "./cleanJob.sh $jobid" >& /dev/null
	
    echo "$jobid" >> ungatheredjob #/GiraphGather.sh $jobid Load >> ${jobid}.log
    mv $logfile ${jobid}.log.stdout
fi
