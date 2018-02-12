#!/bin/bash
#######################workload balance test#####################
#wikitalk
#for dttype in random roulette workload
for dttype in workload
do
    ./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4-label.edgeorientation -qp sq-samelabel -pv 1 -dt $dttype >& .log
    ./clean.sh wiki_workload_log_$dttype .log
done

#google disticd vid=846040 total vertex=846040 edge=7078126
#for dttype in random roulette workload
for dttype in workload
do
    cmd=$(echo "./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4-label.edgeorientation -qp sq-samelabel -pv 1 -dt $dttype")
    echo $cmd
    ./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4-label.edgeorientation -qp sq-samelabel -pv 1 -dt $dttype >& .log
    ./clean.sh google_workload_log_$dttype .log
done


#us-patent: disticd vid=3774768 total vertex=3774768 edge=33037894
#for dttype in random roulette workload
for dttype in workload
do
    cmd=$(echo "./test-tool.sh -g us-patent -a GeneralQueryNL -f us-patent.adj.relabel.data -pn 52 -vm us-patent-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4-label.edgeorientation -qp sq-samelabel -pv 1 -dt $dttype")
    echo $cmd
    ./test-tool.sh -g us-patent -a GeneralQueryNL -f us-patent.adj.relabel.data -pn 52 -vm us-patent-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4-label.edgeorientation -qp sq-samelabel -pv 1 -dt $dttype >& .log
    ./clean.sh us-patent_workload_log_$dttype .log
done

#LJ, the clique-query is not suitable for the workload balance
#for dttype in random roulette workload
for dttype in workload
do
    cmd=$(echo "./test-tool.sh -g soc-LiveJournal -a GeneralQueryNL -f adjformate.relabel.data -pn 81 -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo clique4-label.edgeorientation -qp clique4-label -pv 1 -dt $dttype")
    echo $cmd
    ./test-tool.sh -g soc-LiveJournal -a GeneralQueryNL -f adjformate.relabel.data -pn 81 -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo clique4-label.edgeorientation -qp clique4-label -pv 1 -dt $dttype >& .log
    ./clean.sh lj_workload_log_$dttype .log
done
