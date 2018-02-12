#!/bin/bash
for host in `cat hosts`
do
   echo "ssh $host /home/hadoop/simon/GiraphTaskClean.sh $1"
   ssh $host /home/hadoop/simon/GiraphTaskClean.sh $1
   echo ""
done
