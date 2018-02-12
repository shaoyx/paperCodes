#!/bin/bash
TOOL_HOME=/mnt/disk1/simon0227/simon/test/branches/Tools
graph=$1
filename=$2
hadoop fs -mkdir /user/simon/$graph
hadoop fs -mkdir /user/simon/$graph/graph
hadoop fs -mkdir /user/simon/$graph/edgeindex
hadoop fs -mkdir /user/simon/$graph/vertex2labelmap

hadoop fs -put $TOOL_HOME/graph/$graph/${filename}.adj.relabel.data /user/simon/$graph/graph
hadoop fs -put $TOOL_HOME/graph/$graph/edgeindex-default.relabel /user/simon/$graph/edgeindex
hadoop fs -put $TOOL_HOME/graph/$graph/${filename}-samelabel.relabel /user/simon/$graph/vertex2labelmap
