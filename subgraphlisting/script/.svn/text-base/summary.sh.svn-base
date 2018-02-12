#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Invalid input: lc, rc;"
fi

lc=$1
rc=$2

#root=$(`pwd`)
dir="lc$lc"

for file in `ls $dir`
do
	length=${#file}
	if [ $length -eq 25 ]; then
		./extract.sh $dir/$file
		./caler.sh $dir/$file
		paste -d " " $dir/${file}.er $dir/${file}.stat | awk -v p1=$lc -v p2=$rc '{print p1, p2, $2, $3, $4, $7,$8,$9,$10,$14}' 
	fi
done
