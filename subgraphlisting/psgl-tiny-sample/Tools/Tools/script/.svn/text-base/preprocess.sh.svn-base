#!/bin/bash
###########################################
#INPUT: raw edgelist graph
#
#
#1. clean isolated, loop
#directed --> undirected
#edlist --> adjacency
#ageratee degree and generated remap
#call java to order graph
#do left staff: edgeindex build, vm build
###########################################
TOOL_HOME=/mnt/disk1/simon0227/simon/PAGE/branches/Tools
filein=$1 # raw edgelist
sf=" "
if [ $# -gt 1 ]; then
    sf=$2
fi


#1. clean graph
#echo "Transfer ${filein} into undirected graph, and stored in ${filein}.clean.elist"
#sed -e 's/\t/ /g' ${filein} > ${filein}.tmp
#grep -v % ${filein} | uniq | awk '{if($1 != $2) {print $1,$2; print $2,$1;}}' > ${filein}.good
#sed -e 's///g' ${filein}.good > ${filein}.mid
#sort -n -k 1 ${filein}.mid | uniq > ${filein}.clean.elist

#rm ${filein}.tmp
#rm ${filein}.good
#rm ${filein}.mid

#2. edgelist --> adjacency
echo "Transforming EdgeList: ${filein}.clean.elist to Adjacency: ${filein}.adj"
$TOOL_HOME/script/e2vconvert ${filein}.clean.elist ${filein}.adj
rm ${filein}.clean.elist

#3. remap
#echo "Generating remap....."
#$TOOL_HOME/script/relabelByDegree.sh ${filein}.adj

#4. vertex2labelmap
#awk '{print $1,1,NF-1,0,NF-1}' ${filein}.adj > ${filein}-samelabel.relabel
