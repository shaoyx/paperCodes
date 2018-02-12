package org.apache.giraph.graph;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.conf.Configuration;

public class AccurateWEDistributor implements Distributor {

	int[][] degreeSequence;
	int[][] queryNeighborDist;
	double[] workloadAccumlation;
	int partitionNumber;
	int[] dataVertices;
	double minWorkload;
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
			double tmpWorkload =  Math.pow(degreeSequence[i][0],queryNeighborDist[i][0])
					* Math.pow(degreeSequence[i][1],queryNeighborDist[i][1])
					* Math.pow(degreeSequence[i][2],queryNeighborDist[i][2]);
//			if(Math.abs(degreeSequence[i][0] - tmpWorkload) > 1e-5){
//				System.out.println("Error: not consistent workload.");
//				System.out.println("Degree_A="+degreeSequence[i][0]+" neighbor="+queryNeighborDist[i][0]+
//						"\nDegree_S="+degreeSequence[i][1]+" neighbor="+queryNeighborDist[i][1]+
//						"\nDegree_B="+degreeSequence[i][2]+" neighbor="+queryNeighborDist[i][2]+"\ntmpWorkload="+tmpWorkload);
//				System.exit(-1);
//			}
			double score = Math.sqrt(workloadAccumlation[pid]) + tmpWorkload;
//			double score = degreeSequence[i] + (double)(workloadAccumlation[pid]);
//			double score = degreeSequence[i];
			if(score < minScore){
				minScore = score;
				minWorkload = tmpWorkload;
				targetId = i;
			}
		}
		workloadAccumlation[dataVertices[targetId] % partitionNumber] += minWorkload;
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
		workloadAccumlation = new double[partitionNumber];
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
			ans += ((long)workloadAccumlation[i]) +" ";
		}
//		ans+="\n" + "choose 0:="+choosedTarget[0] +" choose 1:="+choosedTarget[1];
		return ans;
	}

	@Override
	public void setQueryCandidateNeighborDist(int[][] grayNeighborDist) {
		this.queryNeighborDist = grayNeighborDist;
	}

}
