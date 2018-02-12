2089  awk '{if($2 != 10) print $0}' > log
 2090  awk '{if($2 != 10) print $0}' log.log97 > log
  2092  awk 'BEGIN{cpu=0;io=0;total=0;}{cpu+=$4; io+=($5/1000.0); if($2==9){total=cpu+io; print $1,cpu, io, total; cpu=0;io=0;total=0;}' log > log1
	   2093  awk 'BEGIN{cpu=0;io=0;total=0;}{cpu+=$4; io+=($5/1000.0); if($2==9){total=cpu+io; print $1,cpu, io, total; cpu=0;io=0;total=0}' log > log1
		    2094  awk 'BEGIN{cpu=0;io=0;total=0;}{cpu+=$4; io+=($5/1000.0); if($2==9){total=cpu+io; print $1,cpu, io, total; cpu=0;io=0;total=0;}}' log > log1
				 2096  awk 'BEGIN{cpu=0;io=0;total=0;}{cpu+=$4; io+=($5/1000.0); if($2==9){total=(cpu+io)/10; print $1,cpu/10, io/10, total; cpu=0;io=0;total=0;}}' log > log1
				  2104  awk '{if($2 != 10) print $0}' log.log94 > log
				   2107  awk 'BEGIN{cpu=0;io=0;total=0;}{cpu+=$4; io+=($5/1000.0); if($2==9){total=(cpu+io)/10; print $1,cpu/10, io/10, total; cpu=0;io=0;total=0;}}' log > log1
				    2111  awk 'BEGIN{cpu=0;io=0;total=0;}{cpu+=$4; io+=($5/1000.0); if($2==9){total=(cpu+io)/10; print $1,cpu/10, io/10, total; cpu=0;io=0;total=0;}}' log > log1
					 2131  awk '{if($2 != 10) print $0}' log.log101 > log1
					  2132  awk 'BEGIN{cpu=0;io=0;total=0;}{cpu+=$4; io+=($5/1000.0); if($2==9){total=(cpu+io)/10; print $1,cpu/10, io/10, total; cpu=0;io=0;total=0;}}' log1 > log3
					   2158  awk '{if($2 != 10) print $0}' log > logx
