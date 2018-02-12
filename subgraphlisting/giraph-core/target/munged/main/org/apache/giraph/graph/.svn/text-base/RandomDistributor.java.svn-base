package org.apache.giraph.graph;

import org.apache.giraph.utils.Random;
import org.apache.hadoop.conf.Configuration;

public class RandomDistributor implements Distributor {

	@Override
	public void setDegreeSequence(int[][] degrees) {
		// TODO Auto-generated method stub

	}

	@Override
	public int pickTargetId(int range) {
		int targetId;
		do{
			targetId = Math.abs(Random.nextInt()) % range;
		}while(targetId < 0);
		return targetId;
	}

	@Override
	public void initialization(Configuration conf) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDataVerticesSequence(int[] dataVertices) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setQueryCandidateNeighborDist(int[][] grayNeighborDist) {
		// TODO Auto-generated method stub
		
	}

}
