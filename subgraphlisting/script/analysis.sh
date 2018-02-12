#!/bin/bash

file=$1

cat $file | sort -n -k 1 | awk 'BEGIN{curEnd=-1}{if(curEnd == -1) curEnd=$2; if($1 > curEnd) {gap=$1-curEnd; print "gap:", gap; curEnd=$2;}else if(curEnd < $2){curEnd=$2;}}' > $file.gap
