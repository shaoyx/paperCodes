#!/bin/bash
if [ $# -ne 6 ]; then
	echo "input: graph, algorithm, file, partitionNumber, partitionMap, partitionMethod";
	exit;
fi

graph=$1
algorithm=$2
file=$3
pn=$4
#pmf=/user/simon/$graph/partitionmap/uk_2007_05_u_n_incoming_outgoing_weights.mgraph.part.60
#pmf=/user/simon/$graph/partitionmap/uk-2007-05-u-n.hash.part.60
pmf=$5
method=$6
#./changeslaves.sh c
#cmd="stop"
#./stop.sh $cmd
#hadoop jar giraph-examples-0.2-SNAPSHOT-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.tools.graphanalytics.$algorithm -vif org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}InputFormat -vip /user/simon/$graph/graph/$file -wpc org.apache.giraph.partition.${method}PartitionerFactory -ca mapred.child.java.opts=-Xmx10240m,partition.userPartitionCount=$pn,giraph.logLevel="debug",giraph.graphvertexnumber=5,giraph.graphtotalworkload=10 -w $pn
hadoop jar giraph-examples-0.2-SNAPSHOT-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.tools.graphanalytics.$algorithm -vif org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}InputFormat -vip /user/simon/$graph/graph/$file -wpc org.apache.giraph.partition.${method}PartitionerFactory -ca mapred.child.java.opts=-Xmx10240m,partition.userPartitionCount=$pn,giraph.partitionmappath=/user/simon/$graph/partitionmap/$pmf,giraph.graphvertexnumber=105896555,giraph.graphtotalworkload=6603753128 -w $pn
#cmd="start"
#./stop.sh $cmd

