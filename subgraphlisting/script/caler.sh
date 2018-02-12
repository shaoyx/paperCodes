#!/bin/bash

file=$1

head -n 60 $file | awk 'BEGIN{edge=0;cutedge=0}{edge=edge+$4;cutedge=cutedge+$5; print $1,$4, $5, $5/$4, cutedge/edge;}' > ${file}.er 
