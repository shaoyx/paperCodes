package com.graphtools.subgraphmatch;

import org.apache.commons.cli.CommandLine;

import com.graphtools.GenericGraphTool;
import com.graphtools.utils.Graph;
import com.graphtools.utils.GraphAnalyticTool;

@GraphAnalyticTool(
		name = "Edge Degree",
		description = "Edge degree equals to the smaller degree between two end points."
)
public class EdgeDegree 
implements GenericGraphTool{

	private Graph graph;
	
	@Override
	public void run(CommandLine cmd) {
	    graph = new Graph(cmd.getOptionValue("i"), false);
	    double totalEdgeDegree = 0;
		for(int vid : graph.getVertexSet()) {
			for(int nvid : graph.getNeighbors(vid)){
				totalEdgeDegree += Math.min(graph.getDegree(vid), graph.getDegree(nvid));
			}
		}
		System.out.println(String.format("Average Edge Degree: %.2f", totalEdgeDegree / graph.getEdgeSize()));
		System.out.println(String.format("Upper bound replication Factor: %.2f", totalEdgeDegree / graph.getEdgeSize() + 1));
	}

	@Override
	public boolean verifyParameters(CommandLine cmd) {
		// TODO Auto-generated method stub
		return true;
	}

}
