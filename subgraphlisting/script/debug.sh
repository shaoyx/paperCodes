#!/bin/bash
if [ $# -ne 5 ]; then
	echo "input: graph, algorithm, file, partitionNumber, partitionMap";
	exit;
fi

graph=$1
algorithm=$2
file=$3
pn=$4
pmf=$5
cmd="stop"
./stop.sh $cmd
hadoop jar giraph-examples-0.2-SNAPSHOT-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.tools.graphanalytics.$algorithm -vif org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}InputFormat -vip /user/simon/$graph/graph/$file -wpc org.apache.giraph.partition.IdenticalWithRelabelPartitionerFactory -ca giraph.logLevel=debug,partition.userPartitionCount=$pn,giraph.partitionmappath=/user/simon/$graph/partitionmap/$pmf,giraph.graphvertexnumber=5 -w $pn
#cmd="start"
#./stop.sh $cmd
