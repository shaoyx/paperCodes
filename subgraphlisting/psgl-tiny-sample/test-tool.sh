#!/bin/bash

graph=""
algorithm=""
filename=""
pn=24
engine="default"
lc=16
rc=8
outofcore=false
querytype="default"
querygraph=""
vertex2labelmap=""
initpv=""
edgeindex=""
eofile="sample"

help () {
	echo "valid input parameter:"
	echo "	-g	specifiy the graph name"
	echo "	-a	specifiy the algorithm"
	echo "	-f	specifiy the file name of graph"
	echo "	-pn	specifiy the partition number(worker number)"
	echo "	-e	specifiy the execute engine(default, async, page, pagewithqueue)"
	echo "	-lc	specifiy the concurrency of local process unit"
	echo "	-rc	specifiy the concurrency of remote process unit"
	echo "	-qp	specifiy the path of query graph"
	echo "	-qt	specifiy the query type."
	echo "		default: only one query;"
	echo "		general: general version, execute many query at the same time"
	echo "	-vm	specifiy the path of vertex2label map"
	echo "	-ei	specifiy the path of edge index"
	echo "	-eo	specifiy the path of edge orientation"
	echo "	-pv	specifiy initial vertex"
}

parse_opt () {
	#getopts, do not support long argument.
	while true ; do
		if [ $# -le 1 ]; then
			if [ $# -ne 0 ]; then
                echo $# $1 $2
				help
				exit 1
			fi
			return;
		fi
		case $1 in
			"-g")
				graph=$2
				;;
			"-a")
				algorithm=$2
				;;
			"-f") 
				filename=$2
				;;
			"-pn")
				pn=$2
				;;
			"-e")
				engine=$2
				;;
			"-lc")
				lc=$2
				;;
			"-rc")
				rc = $2
				;;
			"-qp")
				querygraph=$2
				;;
			"-qt")
				querytype=$2
				;;
			"-vm")
				vertex2labelmap=$2
				;;
			"-pv")
				initpv=$2
				;;
			"-ei")
				edgeindex=$2
				;;
			"-eo")
				eofile=$2
				;;
			*)
                echo $1, $2
				help
				exit 1
				;;
		esac
		shift 2
	done
}

parse_opt $@

echo $graph $algorithm $filename $querygraph $vertex2labelmap $initpv

if [ "$graph" = "" -o "$algorithm" = "" -o "$filename" = "" -o "$querygraph" = "" -o "$vertex2labelmap" = "" -o "$initpv" = "" ];
then
    echo "failed"
	help
	exit 1
fi
dt=random
echo "hadoop jar giraph-examples-0.2-SNAPSHOT-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.tools.graphanalytics.$algorithm -vif org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}InputFormat -vip /user/simon/$graph/graph/$filename -ca mapred.child.java.opts=-Xmx10240m,partition.userPartitionCount=$pn,giraph.engine=$engine,giraph.asynclocal.concurrency=$lc,giraph.nettyServerExecutionThreads=$rc,giraph.logLevel=info,giraph.subgraphmatch.initpv=$initpv,giraph.distributor.type=$dt -ei /user/simon/$graph/edgeindex/$edgeindex -qp /user/simon/subgraphmatch/querygraph/$querygraph -vl /user/simon/$graph/vertex2labelmap/$vertex2labelmap -w $pn -qt $querytype -qeo /user/simon/subgraphmatch/querygraph/edgeorientation/$eofile"


hadoop jar giraph-examples-0.2-SNAPSHOT-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.tools.graphanalytics.$algorithm -vif org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}InputFormat -vip /user/simon/$graph/graph/$filename -ca mapred.child.java.opts=-Xmx10240m,partition.userPartitionCount=$pn,giraph.engine=$engine,giraph.asynclocal.concurrency=$lc,giraph.nettyServerExecutionThreads=$rc,giraph.logLevel=info,giraph.subgraphmatch.initpv=$initpv,giraph.distributor.type=$dt -ei /user/simon/$graph/edgeindex/$edgeindex -qp /user/simon/subgraphmatch/querygraph/$querygraph -vl /user/simon/$graph/vertex2labelmap/$vertex2labelmap -w $pn -qt $querytype -qeo /user/simon/subgraphmatch/querygraph/edgeorientation/$eofile
