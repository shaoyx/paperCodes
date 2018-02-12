#!/bin/bash
#######################prune test#####################
#us-patent+cycle5cross-label, see the "expr-decomp.sh"
for method in GeneralQueryNLNoPrune
do
    echo "./test-tool.sh -g us-patent -a $method -f us-patent.adj.relabel.data -pn 52 -vm us-patent-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.0 -qp cycle5cross-label -pv 1 -dt workload"
    ./test-tool.sh -g us-patent -a $method -f us-patent.adj.relabel.data -pn 52 -vm us-patent-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo cycle5cross-label.edgeorientation.0 -qp cycle5cross-label -pv 1 -dt workload >& .log
    ./clean.sh prune_us-patent_cycle5cross_$method .log
done

#livejournal: clique4
#for method in GeneralQueryNL GeneralQueryNLNoPrune
#do
#    echo "./test-tool.sh -g soc-LiveJournal -a $method -f adjformate.relabel.data -pn 81 -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo clique4-label.edgeorientation -qp clique4-label -pv 1 -dt workload"
#    ./test-tool.sh -g soc-LiveJournal -a $method -f adjformate.relabel.data -pn 81 -vm soc-lj-samelabel.relabel -il 1 -ei edgeindex-default.relabel -eo clique4-label.edgeorientation -qp clique4-label -pv 1 -dt workload >& .log
#    ./clean.sh prune_livejournal_clique4_$method .log
#done

