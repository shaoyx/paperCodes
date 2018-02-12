package org.apache.giraph.subgraph;

import java.util.ArrayList;
import java.util.Queue;

public class LocalKTrussAlgorithm {
	
	public static int threshold;
	
	/**
	 * Need to remove isolated vertex here!
	 * @param graphStore
	 * @param queue
	 */
	public static void iterativelyPrune(GraphStoreInterface graphStore, Queue<RawEdge> queue){
		while(!queue.isEmpty()){
			RawEdge deleteEdge = queue.poll();
			oneIteration(graphStore, deleteEdge, queue);
		}
	}
	
	public static void initialize(GraphStoreInterface graphStore, Queue<RawEdge> queue){
		for(RawEdge edge : graphStore.getEdges()){
			ArrayList<Integer> nlist1 = graphStore.getVertex(edge.getSmallEndPoint()).getNeighbors();
			ArrayList<Integer> nlist2 = graphStore.getVertex(edge.getBigEndPoint()).getNeighbors();
			int count = 0;
			for(int vid : nlist1){
				if(nlist2.contains(vid)){
					count++;
				}
			}
			if(count < threshold - 2){
				queue.add(edge);
				edge.delete();
			}
			else{
				edge.setCount(count);
			}
		}
	}
	
	public static void oneIteration(GraphStoreInterface graphStore, RawEdge deleteEdge, Queue<RawEdge> queue){
		int firstEndPoint = deleteEdge.getSmallEndPoint();
		int secondEndPoint = deleteEdge.getBigEndPoint();
		ArrayList<Integer> nlist1 = graphStore.getVertex(firstEndPoint).getNeighbors();
		ArrayList<Integer> nlist2 = graphStore.getVertex(secondEndPoint).getNeighbors();
		for(int vid : nlist1){
			if(nlist2.contains(vid)){
				if(!graphStore.getEdge(firstEndPoint, vid).isDelete()){
					if(graphStore.getEdge(firstEndPoint, vid).decAndGetCount() < threshold){
						queue.add(graphStore.getEdge(firstEndPoint, vid));
					}
				}
				if(!graphStore.getEdge(secondEndPoint, vid).isDelete()){
					if(graphStore.getEdge(secondEndPoint, vid).decAndGetCount() < threshold){
						queue.add(graphStore.getEdge(secondEndPoint, vid));
					}
				}
			}
		}
	}

	public static void Main(String[] args){
		
	}
}
