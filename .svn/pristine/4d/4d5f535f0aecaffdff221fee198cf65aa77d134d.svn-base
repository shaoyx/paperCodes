package org.apache.giraph.tools.graphanalytics.simrank;

public class SimRankPair implements Comparable{
	private int vertex;
	private double simrank;
	
	public SimRankPair(int vid, double sr){
		vertex  = vid;
		simrank = sr;
	}
	
	public int getVertex(){
		return vertex;
	}
	
	public double getSimRank(){
		return simrank;
	}

	@Override
	public int compareTo(Object arg0) {
		SimRankPair other = (SimRankPair) arg0;
		double delta = this.simrank - other.simrank;
		return delta < 0 ? -1 : 1;
	}
}
