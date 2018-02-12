#!/bin/bash
filein=$1
sf=$2

echo "Transfer $filein into undirected graph, and stored in $fileout"

awk -F $sf '{if($1!=$2) {print $1, $2; print $2,$1;} }' $filein > $fileout.good
sed -e 's///g' $fileout.good > $fileout.mid
sort -n -k 1 $fileout.mid | uniq -u > $fileout.clean.elist

rm $fileout.good
rm $fileout.mid
