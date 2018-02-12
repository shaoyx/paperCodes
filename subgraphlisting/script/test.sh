if [ $# -lt 5 ]; then
	echo "input: graph, algorithm, file, partitionNumber, engine, lconcurrency, rconcurrency";
	exit;
fi

graph=$1
algorithm=$2
file=$3
pn=$4
engine=$5
if [ $# -lt 6 ]; then
	lconcurrency=16
	rconcurrency=8
else
	lconcurrency=$6
	rconcurrency=$7
fi
#./changeslaves.sh c
cmd="stop"
#./stop.sh $cmd
hadoop jar giraph-examples-0.2-SNAPSHOT-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.tools.graphanalytics.$algorithm -vif org.apache.giraph.tools.graphanalytics.$algorithm\$${algorithm}InputFormat -vip /user/simon/$graph/graph/$file -ca mapred.child.java.opts=-Xmx10240m,partition.userPartitionCount=$pn,giraph.engine=$engine,giraph.asynclocal.concurrency=$lconcurrency,giraph.nettyServerExecutionThreads=$rconcurrency -w $pn
#cmd="start"
#./stop.sh $cmd
