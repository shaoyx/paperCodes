#!/bin/bash
file=$1
cat $file | grep "|" | awk '{if($2 != 10){print $0;}}' > ${file}.clean
awk 'BEGIN{
		cpu=0;
		io=0;
		total=0;
		localcost=0;
		remotecost=0;
		lc=0;
		rc=0;
		incomingcost=0;
		blockwaitcost=0;
	} 
	{
		cpu+=$4; 
		io+=($5/1000.0); 
		localcost+=($13/1000.0); 
		lc+=$12; 
		rc+=$16; 
		remotecost+=($17/1000.0); 
		if(NF >= 20) { 
			incomingcost+=($22/1000.0); 
			blockwaitcost+=($20/1000.0);
		} 
		if($2==9) 
		{ 
			total=(cpu+io)/10; 
			if(NF >= 20) { 
				print $1,cpu/10, io/10, localcost/10, remotecost/10, total,lc/10,rc/10, blockwaitcost/10, incomingcost/10, incomingcost*1000.0/lc; 
			} 
			else { 
				print $1,cpu/10, io/10, localcost/10, remotecost/10, total,lc/10,rc/10; 
			} 
			cpu=0;io=0;total=0;localcost=0;remotecost=0;lc=0;rc=0; incomingcost=0;blockwaitcost=0;
		}
	}' ${file}.clean > ${file}.stat
