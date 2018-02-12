#!/bin/bash

PMLogProcess () {
	logdir=$1;
	logfile=$2;

	cat ~/hdfs/logs/userlogs/$logdir/$logfile
	return;
}

RequestLogProcess () {
	logdir=$1;
	logfile=$2;

#local variable
	superstep=-1;
	requestdest="";
	requestsize=0;
	requeststart=0;
	requeststop=0;
	resuestlast=0;
	requestcnt=0;
	minvalue=-1;
	maxvalue=-1;
	
	while read line
	do
		if [ "$(expr match "$line" "Superstep")" -gt 0 ]; then
			if [ $superstep -ne -1 ]; then
				echo $superstep $requestcnt $minvalue $maxvalue
			fi
			superstep=${line#Superstep=}
			requestcnt=0;
			minvalue=-1
			maxvalue=-1
		elif [ "$(expr match "$line" "RequestDest")" -gt 0 ]; then
			tmp=${line#RequestDest=}
			requestdest=${tmp%RequestSize=*}

			tmp=${tmp#*RequestSize=}
			requestsize=${tmp%RequestStart=*}

			tmp=${tmp#*RequestStart=}
			requeststart=${tmp%RequestStop=*}
			tmp=${tmp#*RequestStop=}
			requeststop=${tmp%RequestCost=*}
			tmp=${tmp#*RequestCost=}
			requestlast=$tmp
			((requestcnt++));
			if [ $minvalue -eq -1 -o $minvalue -gt $requeststart ]; then
				minvalue=$requeststart
			fi
			if [ $maxvalue -eq -1 -o $maxvalue -lt $requeststop ]; then
				maxvalue=$requeststop
			fi
			echo $requestdest $requestsize $requeststart $requeststop $requestlast
	   elif [ "$(expr match "$line" "log4j:WARN")" -eq 0 ]; then
			echo $line
	   fi
	done< <(cat ~/hdfs/logs/userlogs/$logdir/$logfile)
	if [ $superstep -ne -1 ]; then
		echo $superstep $requestcnt $minvalue $maxvalue
	fi
}

TaskLogProcess () {
	logdir=$1;
	logfile=$2;
	empty=""
	result=$HOSTNAME;
	while read line
	do
		 key=${line%=*}
		 value=${line#*=}
		 empty="not empty"
		 case $key in
			"retrieveNeighborCost" )
				result="$result $value";;
			"retrieveNeighborCount" )
				result="$result $value";;
			"broadcastTime" )
				result="$result $value";;
			"totalUpdateCost" )
				result="$result $value";;
			"dhtInsertCost" )
				result="$result $value";;
			"nullCount" )
				result="$result $value";;
			"getPartitionstatistics" )
				result="$result $value";;
			"updatePartitionStatistics" )
				result="$result $value";;
			"processedVertex" )
				result="$result $value";;
			"result" )
				result="$result $value";;
			"IO cost" )
				result="$result $value";;
			"WorkerHost" )
				result="$result $value";;
			"WorkerVertexNumber" )
				result="$result $value";;
			"workerEdgeNumber" )
				result="$result $value";;
			"workerEdgeCuts" )
				result="$result $value";;
			"workerInternalEdge" )
				result="$result $value";;
			"memoryUsed" )
				result="$result $value";;
			"partitionEdgeCuts" )
				result="$result $value";;
			"partitionSumDegreeSquare" )
				result="$result $value";;
			"localRequestG" )
				result="$result $value";;
			"remoteRequestG" )
				result="$result $value";;
			"localRequestI" )
				result="$result $value";;
			"remoteRequestI" )
				result="$result $value";;
			"bitsetTimeCost" )
				result="$result $value";;
			"workloadTimeCost" )
				result="$result $value";;
		 esac
	done< <(cat ~/hdfs/logs/userlogs/$logdir/$logfile)
	if [ -n "$empty" ]; then
		echo $result
	fi
}

LoadLogProcess () {
	logdir=$1;
	logfile=$2;
	result=$HOSTNAME
	target=""

	reg=".*call: Loaded.*";
	reg2=".*call: Computation.*";

	splitnum=""
	totalcost=""
	vspeed=""
	espeed=""

	superstep=""
	timecost=""
	partitions=""
	waitForRequest=""
	aggregator=""
	waitForWorker=""
	localRequest=""
	remoteRequest=""
	localRequestCost=""
	remoteRequestCost=""
	localByteCount=""
	remoteByteCount=""
	waitForBlockingQueue=""
IncomingMessageProcessCost=""
IncomingMessageWaitCost=""
IncomingMessageCount=""

	while read line
	do
   if [ "$(expr match "$line" "$reg")" -gt 0 ]; then
		target=$line;

		tmp=${target#*Loaded}
		splitnum=${tmp%%input*}

		tmp=${tmp##*in}
		totalcost=${tmp%%secs*}

		tmp=${tmp#*)}
		vspeed=${tmp%vertices/sec*}

		tmp=${tmp##*,}
		espeed=${tmp%edges/sec*}
		superstep=-1

		echo "$result -1 $splitnum $totalcost $vspeed $espeed"
	elif [ "$(expr match "$line" "$reg2")" -gt 0 ]; then
		target=$line
		
		tmp=${target#*took}
		timecost=${tmp%secs*}

		tmp=${tmp#*for}
		partitions=${tmp%partitions*}

		tmp=${tmp#*superstep}
		superstep=${tmp%.*}
#	echo "$result $superstep $timecost $partitions"
	elif [ "$(expr match "$line" ".*waitForRequest.*")" -gt 0 ]; then
		target=$line

		tmp=${target#*superstep=}
#		superstep=${tmp%waitForRequest*}

		tmp=${tmp#*waitForRequest=}
		waitForRequest=$tmp
#	echo "$result $superstep $waitForRequest"
	elif [ "$(expr match "$line" ".*waitForBlockingQueue.*")" -gt 0 ]; then
		target=$line

		tmp=${target#*superstep=}
#		superstep=${tmp%waitForRequest*}

		tmp=${tmp#*waitForBlockingQueue=}
		waitForBlockingQueue=$tmp
	elif [ "$(expr match "$line" ".*IncomingMessageProcessCost.*")" -gt 0 ]; then
		target=$line

		tmp=${target#*superstep=}
#		superstep=${tmp%waitForRequest*}

		tmp=${tmp#*IncomingMessageProcessCost=}
		IncomingMessageProcessCost=$tmp
	elif [ "$(expr match "$line" ".*IncomingMessageWaitCost.*")" -gt 0 ]; then
		target=$line

		tmp=${target#*superstep=}
#		superstep=${tmp%waitForRequest*}

		tmp=${tmp#*IncomingMessageWaitCost=}
		IncomingMessageWaitCost=$tmp
	elif [ "$(expr match "$line" ".*IncomingMessageCount.*")" -gt 0 ]; then
		target=$line

		tmp=${target#*superstep=}
#		superstep=${tmp%waitF*}

		tmp=${tmp#*IncomingMessageCount=}
		IncomingMessageCount=$tmp
	elif [ "$(expr match "$line" ".*aggregator.*")" -gt 0 ]; then
		target=$line

		tmp=${target#*superstep=}
#		superstep=${tmp%waitForRequest*}

		tmp=${tmp#*aggregator=}
		aggregator=$tmp
#	echo "$result $superstep $waitForRequest"
	elif [ "$(expr match "$line" ".*localRequest=.*")" -gt 0 ]; then
	    localRequest=${line#*=}
	elif [ "$(expr match "$line" ".*localRequestCost=.*")" -gt 0 ]; then
	    localRequestCost=${line#*=}
	elif [ "$(expr match "$line" ".*localByteCount=.*")" -gt 0 ]; then
	    localByteCount=${line#*=}
	elif [ "$(expr match "$line" ".*remoteRequest=.*")" -gt 0 ]; then
	    remoteRequest=${line#*=}
	elif [ "$(expr match "$line" ".*remoteRequestCost=.*")" -gt 0 ]; then
	    remoteRequestCost=${line#*=}
	elif [ "$(expr match "$line" ".*remoteByteCount=.*")" -gt 0 ]; then
		remoteByteCount=${line#*=}
	elif [ "$(expr match "$line" ".*waitForOtherWorkers.*")" -gt 0 ]; then
		target=$line

		tmp=${target#*superstep=}

		tmp=${tmp#*waitForOtherWorkers=}
		waitForWorker=$tmp

		if [ $superstep -gt -1 ]; then
			echo "$result $superstep | $timecost $waitForRequest $aggregator $waitForWorker | $localRequest $localRequestCost $localByteCount | $remoteRequest $remoteRequestCost $remoteByteCount | $waitForBlockingQueue $IncomingMessageProcessCost $IncomingMessageWaitCost $IncomingMessageCount";
		fi
	fi
	done< <(cat ~/hdfs/logs/userlogs/$logdir/$logfile)
}

###########################################
##############  Main Function #############
###########################################
if [ $# -ne 2 ]; then
	echo "Please specify: jobid, logprocesstype(Request, Load, Task)"
	exit
fi
job=$1
tasktype=$2

taskSig=${job#job_}
attempTaskPrefix=attempt_$taskSig

totaldirs=$(ls ~/hdfs/logs/userlogs | grep $attempTaskPrefix);
#echo $totaldirs

dirsize=${#totaldirs}

#echo "dirsize=$dirsize"
size=0;
declare -a dir;

for((index=1;index < dirsize; index=index+37))
do
#echo $size
 dir[$size]="$(expr substr "$totaldirs" $index 36)"
((size++));
done

size=${#dir[@]}

for((index=0;index<size;index++))
do
	case $tasktype in
		"Request" )
			echo -e "\e[1;32mRequestLogProcess ${dir[$index]} stderr\e[0m" 1>&2
			RequestLogProcess ${dir[$index]} stderr
			;;
		"Task" )
			echo -e "\e[1;32mTaskLogProcess ${dir[$index]} stdout\e[0m" 1>&2
			TaskLogProcess ${dir[$index]} stdout
			;;
		"Load" )
			echo -e "\e[1;32mLoadLogProcess ${dir[$index]} syslog\e[0m" 1>&2
			LoadLogProcess ${dir[$index]} syslog
			;;
		"PM" )
			echo -e "\e[1;32mPMLogProcess ${dir[$index]} stderr\e[0m" 1>&2
			PMLogProcess ${dir[$index]} stderr
			;;
	esac
done
