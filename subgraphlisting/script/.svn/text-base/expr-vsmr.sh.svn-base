#!/bin/bash
#######################performance test#####################
############# livejournal #####################
PG=("tc-samelabel" "sq-samelabel" "sq-samelabel" "sq-samelabel" "cycle4cross-label" "clique4-label" "cycle5cross-label" "cycle4cross-label" "cycle4cross-label")
EO=("cycle3-samelabel.edgeorientation" "cycle4-closure-one.edgeorientation" "cycle4-closure-two.edgeorientation" "cycle4-closure-three.edgeorientation" "cycle4cross-label.edgeorientation.0" "clique4-label.edgeorientation" "cycle5cross-label.edgeorientation.0" "cycle4cross-label.edgeorientation.1" "cycle4cross-label.edgeorientation.2")
PN=(27 81 81 81 81 81 81 81 81)
PV=(1 1 4 3 1 1 1 1 2)

for((id=9; id<9;id++))
do
    echo "./test-tool.sh -g soc-LiveJournal -a GeneralQueryNL -f adjformate.relabel.data -pn ${PN[$id]} -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload"
    ./test-tool.sh -g soc-LiveJournal -a GeneralQueryNL -f adjformate.relabel.data -pn ${PN[$id]} -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload >& .log
    ./clean.sh vsmr_livejournal_${PG[$id]}_${PN[$id]}_${EO[$id]}_${PV[$id]} .log
done
#exit
# increase the memory
#PG=("tc-samelabel" "sq-samelabel" "cycle4cross-label" "clique4-label" "cycle5cross-label")
#EO=("cycle3-samelabel.edgeorientation" "cycle4-label.edgeorientation" "cycle4cross-label.edgeorientation.0" "clique4-label.edgeorientation" "cycle5cross-label.edgeorientation.0")
#PN=(27 81 52 52 52)
#PV=(1 1 1 1 1)
#
############## web-google #####################
#for((id=4; id<5;id++))
#do
#    echo "./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn ${PN[$id]} -vm google-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload"
#    ./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn ${PN[$id]} -vm google-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload >& .log
#    ./clean.sh vsmr_google_${PG[$id]}_${PN[$id]}_${EO[$id]}_${PV[$id]} .log
#done
#
############## us-patent #####################
#for((id=4; id<5;id++))
#do
#    echo "./test-tool.sh -g us-patent -a GeneralQueryNL -f us-patent.adj.relabel.data -pn ${PN[$id]} -vm us-patent-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload"
#    ./test-tool.sh -g us-patent -a GeneralQueryNL -f us-patent.adj.relabel.data -pn ${PN[$id]} -vm us-patent-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload >& .log
#    ./clean.sh vsmr_us-patent_${PG[$id]}_${PN[$id]}_${EO[$id]}_${PV[$id]} .log
#done
#
############## wikitalk #####################
#for((id=4; id<5;id++))
#do
#    echo "./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn ${PN[$id]} -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload"
#    ./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn ${PN[$id]} -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload >& .log
#    ./clean.sh vsmr_wikitalk_${PG[$id]}_${PN[$id]}_${EO[$id]}_${PV[$id]} .log
#done
#
PG=("cycle5cross-label" "cycle5cross-label" "cycle5cross-label" "cycle5cross-label")
EO=("cycle5cross-label.edgeorientation.1" "cycle5cross-label.edgeorientation.2" "cycle5cross-label.edgeorientation.3" "cycle5cross-label.edgeorientation.4")
PN=(55 55 55 55)
PV=(1 2 3 4)
WL=("workload" "workload" "workload" "aworkload")

############## wikitalk #####################
for((id=0; id<3;id++))
do
    echo "./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn ${PN[$id]} -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload"
    ./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn ${PN[$id]} -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload >& .log
    ./clean.sh vsmr_wikitalk_${PG[$id]}_${PN[$id]}_${EO[$id]}_${PV[$id]} .log
done

exit

############# web-google #####################
for((id=3; id<4;id++))
do
    echo "./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn ${PN[$id]} -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt workload"
    ./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn ${PN[$id]} -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo ${EO[$id]} -qp ${PG[$id]} -pv ${PV[$id]} -dt aworkload >& .log
    ./clean.sh vsmr_google_${PG[$id]}_${PN[$id]}_${EO[$id]}_${PV[$id]} .log
done


################ PG5 ##########################





#line4
#line5

#wikitalk
#pv=2
#for query in line4 line5
#do
#    echo "./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${query}-relabel.edgeorientation -qp ${query}-relabel -pv $pv -dt workload"
#    ./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${query}-relabel.edgeorientation -qp ${query}-relabel -pv $pv -dt workload >& .log
#    ./clean.sh vsmr_wikitalk_${query}_$pv .log
#    ((pv++))
#done
#

#livejournal
#pv=2
#for query in 8 27 64 125
#do
#    echo "./test-tool.sh -g soc-LiveJournal -a GeneralQueryNL -f adjformate.relabel.data -pn $query -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle3-samelabel.edgeorientation -qp tc-samelabel -pv 1 -dt workload"
#    ./test-tool.sh -g soc-LiveJournal -a GeneralQueryNL -f adjformate.relabel.data -pn $query -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle3-samelabel.edgeorientation -qp tc-samelabel -pv 1 -dt workload >& .log
#    ./clean.sh vsmr_livejournal_tc_$query .log
#done
#exit
#
#pv=2
#for query in line4
#do
#    echo "./test-tool.sh -g soc-LiveJournal -a GeneralQueryNL -f adjformate.relabel.data -pn 81 -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${query}-relabel.edgeorientation -qp ${query}-relabel -pv $pv -dt workload"
#    ./test-tool.sh -g soc-LiveJournal -a GeneralQueryNL -f adjformate.relabel.data -pn 81 -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo ${query}-relabel.edgeorientation -qp ${query}-relabel -pv $pv -dt workload >& .log
#    ./clean.sh vsmr_livejournal_${query}_$pv .log
#    ((pv++))
#done
#
