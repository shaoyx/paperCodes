#!/bin/bash
file=$1
awk '{print $1, NF-1}' $file | sort -n -k 2 | awk 'BEGIN{id=1;}{print $1, id; id++}' > ${file}.relabel

exit

declare -a remap
while read line
do
  key=${line% *}
  value=${line#* }
 # echo $key, $value
  remap[$key]=$value
done < <(cat ${file}.relabel)

#for v in ${!remap[@]}
#do
#echo $v, ${remap[$v]}
#done
rm ${file}.relabel.data >& /dev/null
#cat $file
while read line
do
  nline=""
#  echo $line
  #parse a line, use tr to delete all the cntrol character
  for id in `echo $line | tr -d [:cntrl:]`
  do
    #print $id
    #echo love $id
   nline="$nline ${remap[$id]}"
#echo line: $nline
  done
#  echo $nline
  echo $nline >> ${file}.relabel.data
done< $file
