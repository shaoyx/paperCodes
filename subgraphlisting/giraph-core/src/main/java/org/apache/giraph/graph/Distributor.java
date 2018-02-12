package org.apache.giraph.graph;

import org.apache.hadoop.conf.Configuration;

public interface Distributor {
	
	public void initialization(Configuration conf);
	
	public void setDegreeSequence(int[][] degrees);
	
	public void setDataVerticesSequence(int[] dataVertices);
	
	public int pickTargetId(int range);

	public void setQueryCandidateNeighborDist(int[][] grayNeighborDist);
}