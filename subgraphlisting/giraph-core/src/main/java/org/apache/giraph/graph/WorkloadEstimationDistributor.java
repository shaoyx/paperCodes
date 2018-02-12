package org.apache.giraph.graph;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.conf.Configuration;

public class WorkloadEstimationDistributor implements Distributor {

	int[][] degreeSequence;
	long[] workloadAccumlation;
	int partitionNumber;
	int[] dataVertices;
	long minWorkload;
	int minWorkloadId;
	
	/*for debug*/
//	int[] choosedTarget;

	@Override
	public void setDegreeSequence(int[][] degrees) {
		degreeSequence = degrees;
	}

	@Override
	public int pickTargetId(int range) {
		/*TODO: Assume HASH Partition here. */
		int targetId = 0;
		double minScore = Double.MAX_VALUE;
		for(int i = 0; i < range; i++){
			int pid = dataVertices[i] % partitionNumber;
			/*delta * currentWorkload / minWorkload */
//			double score = degreeSequence[i] * workloadAccumlation[pid] * 1.0 / minWorkload;
			double score = degreeSequence[i][0] + Math.sqrt((double)(workloadAccumlation[pid]));
//			double score = degreeSequence[i][0] + Math.log((double)(workloadAccumlation[pid]));
//			Math.lo
//			double score = degreeSequence[i][0] + (double)(workloadAccumlation[pid]);
//			double score = degreeSequence[i][0];
			if(score < minScore){
				minScore = score;
				targetId = i;
			}
		}
		workloadAccumlation[dataVertices[targetId] % partitionNumber] += degreeSequence[targetId][0];
//		if(minWorkloadId == targetId){
//			for(int i = 0; i < range; i++){
//				if(workloadAccumlation[i] < minWorkload){
//					minWorkload = workloadAccumlation[i];
//				}
//			}
//		}
//		choosedTarget[targetId] ++;
		return targetId;
	}

	@Override
	public void initialization(Configuration conf) {
		partitionNumber = conf.getInt(GiraphConstants.USER_PARTITION_COUNT, 
				GiraphConstants.DEFAULT_USER_PARTITION_COUNT);
		workloadAccumlation = new long[partitionNumber];
		for(int i = 0; i < workloadAccumlation.length; i++){
			workloadAccumlation[i] = 1; // smooth the value.
		}
		minWorkload = 1;
		minWorkloadId = 0;
//		choosedTarget = new int[2];
//		choosedTarget[0] = choosedTarget[1] = 0;
	}

	@Override
	public void setDataVerticesSequence(int[] dataVertices) {
		this.dataVertices = dataVertices;
	}
	
	public String toString(){
		String ans = "";
		for(int i = 0; i < partitionNumber; i++){
			ans += workloadAccumlation[i] +" ";
		}
//		ans+="\n" + "choose 0:="+choosedTarget[0] +" choose 1:="+choosedTarget[1];
		return ans;
	}

	@Override
	public void setQueryCandidateNeighborDist(int[][] grayNeighborDist) {
		// TODO Auto-generated method stub
		
	}

}
