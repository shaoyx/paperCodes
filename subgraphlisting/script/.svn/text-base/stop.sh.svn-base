#!/bin/bash

if [ "$1" == "all" ]; then
	for host in `cat hosts`
	do
		ssh $host /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh start datanode
		ssh $host /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh start tasktracker
	done
else
	ssh changping36 /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh $1 tasktracker
	ssh changping38 /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh $1 tasktracker
	ssh changping41 /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh $1 tasktracker
fi
sleep 5

#five more tasktracker
#ssh changping60 /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh $1 tasktracker
#ssh changping21 /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh $1 tasktracker
#ssh changping18 /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh $1 tasktracker
#ssh changping50 /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh $1 tasktracker
#ssh changping44 /home/hadoop/hadoopinstall/hadoop-0.20.2/bin/hadoop-daemon.sh $1 tasktracker
