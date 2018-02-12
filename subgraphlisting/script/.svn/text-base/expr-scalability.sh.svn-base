#!/bin/bash
#######################scalability test#####################
#wikitalk
for pnum in 80 70 60 50 40 30 20 10
do
    echo "./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn $pnum -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4-label.edgeorientation -qp sq-samelabel -pv 1 -dt workload"
    ./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn $pnum -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4-label.edgeorientation -qp sq-samelabel -pv 1 -dt workload >& .log
    ./clean.sh scalability_wikitalk_$pnum .log
done
