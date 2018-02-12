#!/bin/bash
#######################prune test#####################
#wikitalk+cycle4cross-label
#echo "./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4cross-label.edgeorientation.0 -qp cycle4cross-label -pv 2 -dt workload"
#./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4cross-label.edgeorientation.0 -qp cycle4cross-label -pv 2 -dt workload >& .log
#./clean.sh decomp_wikitalk_cycle4cross_0_2 .log
#for pnum in 1 2
#do
#    echo "./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4cross-label.edgeorientation.$pnum -qp cycle4cross-label -pv $pnum -dt workload"
#    ./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle4cross-label.edgeorientation.$pnum -qp cycle4cross-label -pv $pnum -dt workload >& .log
#    ./clean.sh decomp_wikitalk_cycle4cross_${pnum}_${pnum} .log
#done
#

#wikitalk+cycle5cross-label
#echo "./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.0 -qp cycle5cross-label -pv 1 -dt workload"
#./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.0 -qp cycle5cross-label -pv 1 -dt workload >& .log
#./clean.sh decomp_wikitalk_cycle5cross_0_1 .log
#for pnum in 1 2 3 4
#do
#    echo "./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.$pnum -qp cycle5cross-label -pv $pnum -dt workload"
#    ./test-tool.sh -g wikitalk -a GeneralQueryNL -f wiki-talk.undir.adj.relabel.data -pn 52 -vm wikitalk-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.$pnum -qp cycle5cross-label -pv $pnum -dt workload >& .log
#    ./clean.sh decomp_wikitalk_cycle5cross_${pnum}_${pnum} .log
#done

#google:cycle5cross-label
#echo "./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.0 -qp cycle5cross-label -pv 1 -dt aworkload"
#./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.0 -qp cycle5cross-label -pv 1 -dt aworkload >& .log
#./clean.sh decomp_google_cycle5cross_0_1 .log
##./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.1 -qp cycle5cross-label -pv 1 -dt workload >& .log
#./clean.sh test_decomp_google_cycle5cross_1_1 .log
for pnum in 1 2 3 4
do
    echo "./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo tree5-label.edgeorientation.$pnum -qp tree5-label -pv $pnum -dt aworkload"
    ./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo tree5-label.edgeorientation.$pnum -qp tree5-label -pv $pnum -dt aworkload >& .log
    ./clean.sh test_decomp_google_tree5_${pnum}_${pnum} .log
done

for id in 1 2 3 4
do
    echo "./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo tree5-label.edgeorientation.0 -qp tree5-label -pv $id -dt aworkload"
    ./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo tree5-label.edgeorientation.0 -qp tree5-label -pv $id -dt aworkload >& .log
    ./clean.sh test_decomp_google_tree5_pv_$id .log
done

#echo "./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.0 -qp cycle5cross-label -pv 1 -dt aworkload"
#./test-tool.sh -g google -a GeneralQueryNL -f google.adj.relabel.data -pn 52 -vm google-samelabel.bs.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.0 -qp cycle5cross-label -pv 1 -dt workload >& .log
#./clean.sh decomp_google_cycle5cross_0_1 .log
