#!/bin/bash

graph=""
algorithm="" #the class name of the vertex program, which can be found in org.apache.giraph.tools.graphanalytics package
filename="" #the file name of graph
pn=24 #the number of partition
engine="default" #the type of engine
lc=16 #the (initial) number of local message processors
rc=8  #the (initial) number of remote message processors
dynamic=false

mappath="/user/simon/uk-2007-05-u/partitionmap/uk-2007-05-u-n.hash.part.60" #the partition scheme which is obtained in offline by your own partition algorihtm.
vnum=105896560  # the number of vertices.
handler="requestFrameDecoder"
partitionerFactory="hash"

help () {
	echo "valid input parameter:"
	echo "	-g	specifiy the graph name"
	echo "	-a	specifiy the algorithm"
	echo "	-f	specifiy the file name of graph"
	echo "	-d	specifiy dccm"
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
	echo "	-il	specifiy initial label"
    echo "	-mp specifiy the map file path"
    echo "	-pf specifiy the partitioner factor. [default: hash]"
}

parse_opt () {
	#getopts, do not support long argument.
	while true ; do
		if [ $# -le 1 ]; then
			if [ $# -ne 0 ]; then
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
			"-d")
				dynamic=$2
				;;
			"-lc")
				lc=$2
				;;
			"-rc")
				rc=$2
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
			"-il")
				initLabel=$2
				;;
			"-ei")
				edgeindex=$2
				;;
			"-eo")
				eofile=$2
                ;;
			"-mp")
				mappath="$2"
				;;
			"-pf")
				partitionerFactory=$2
				;;
			*)
				help
				exit 1
				;;
		esac
		shift 2
	done
}

parse_opt $@

echo $graph $algorithm $filename

if [ "$graph" = "" -o "$algorithm" = "" -o "$filename" = "" ];
then
	help
	exit 1
fi

#hadoop fs -rmr /user/simon/subgraphmatch/results
#outputdir="-of org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}OutputFormat -op /user/simon/subgraphmatch/results/semi-resultsr21"
outputdir=""

memory=15360

addition_opt=""

if [ "$algorithm" = "SemiClusteringVertex" -o "$algorithm" = "CompressSemiClusteringVertex" ];
then
    addition_opt=",vertex.max.cluster.count=3,semicluster.max.message.sent.count=1,semicluster.max.vertex.count=5,semicluster.max.supersteps=10,giraph.msgRequestSize=1024"
fi

if [ "$algorithm" = "DiameterEstimator" ];
then
    addition_opt=",diameter.bitmaskarraysize=1"
fi

if [ "$algorithm" = "BreadthFirstSearch" ];
then
    addition_opt=",bfs.sourceid=57"
fi

if [ "$algorithm" = "SimplePageRankVertex" ];
then
    addition_opt=""
fi

if [ "$algorithm" = "SingleSourceShortestPathVertex" ];
then
    addition_opt=",sssp.sourceid=10"
fi

if [ "$partitionerFactory" = "hash" ];
then
    addition_opt="${addition_opt},giraph.graphPartitionerFactoryClass=org.apache.giraph.partition.HashPartitionerFactory"
else
    mappath="/user/simon/$graph/partitionmap/truss/$mappath" 
    addition_opt="${addition_opt},giraph.partitionmappath=$mappath"
fi

echo "hadoop jar giraph-examples-0.2-SNAPSHOT-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.tools.graphanalytics.$algorithm -vif org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}InputFormat -vip /user/simon/$graph/graph/$filename $outputdir -ca mapred.child.java.opts=-Xmx${memory}m,partition.userPartitionCount=$pn,giraph.engine=$engine,giraph.asynclocal.concurrency=$lc,giraph.nettyServerExecutionThreads=$rc,giraph.nettyServerThreads=$rc,giraph.logLevel=info,giraph.isdynamic=$dynamic,giraph.nettyServerExecutionAfterHandler=$handler${addition_opt} -w $pn"
hadoop jar giraph-examples-0.2-SNAPSHOT-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.tools.graphanalytics.$algorithm -vif org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}InputFormat -vip /user/simon/$graph/graph/$filename $outputdir -ca mapred.child.java.opts=-Xmx${memory}m,partition.userPartitionCount=$pn,giraph.engine=$engine,giraph.asynclocal.concurrency=$lc,giraph.nettyServerExecutionThreads=$rc,giraph.nettyServerThreads=$rc,giraph.logLevel=info,giraph.isdynamic=$dynamic,giraph.nettyServerExecutionAfterHandler=$handler${addition_opt} -w $pn
