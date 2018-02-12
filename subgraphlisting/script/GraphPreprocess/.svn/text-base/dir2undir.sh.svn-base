#!/bin/bash
filein=$1
fileout=$2

echo "Transfer $filein into undirected graph, and stored in $fileout"

#awk '{print $1,$2; print $2,$1;}' $filein | sort -u | sort -n > $fileout
awk '{if($1!=$2) {print $1,$2; print $2,$1;}}' $filein > $fileout.tmp
 sed -e 's///g' $fileout.tmp > $fileout.good
#cat $filein > $fileout.merge
#cat $fileout.tmp
#echo ""
cat $fileout.good | sort -u | sort -n > $fileout #sort -u -n -c -k 1,2
rm $fileout.tmp
rm $fileout.good


