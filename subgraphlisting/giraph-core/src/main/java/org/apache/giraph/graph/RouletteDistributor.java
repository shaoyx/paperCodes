package org.apache.giraph.graph;

import org.apache.hadoop.conf.Configuration;


public class RouletteDistributor implements Distributor {
	
	public static final java.util.Random rand = new java.util.Random();
	int[][] degreeSequence;
//	double totalDegree;

	@Override
	public void setDegreeSequence(int[][] degrees) {
		degreeSequence = degrees;
//		for(int i = 0; i < degrees.length; i++){
//			totalDegree += 1.0/distribution[i];
//		}
	}

	@Override
	public int pickTargetId(int range) {
		double [] distribution = new double[range];
		double totalDegree = 0;
		for(int i = 0; i < range; i++){
			distribution[i] = 1.0 / degreeSequence[i][0];
			totalDegree += distribution[i];
		}
		double randomNumber = rand.nextDouble();
//		System.out.println("randomNumber="+randomNumber);
		double sum = 0.0;
		for(int i = 0; i < distribution.length; i++){
			sum += distribution[i]/totalDegree;
//			System.out.println("\ti="+i+": "+distribution[i]+", normal="+normal+", sum="+sum);
			if(sum > randomNumber){
//				System.out.println();
				return i;
			}
		}
		return distribution.length - 1;
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
