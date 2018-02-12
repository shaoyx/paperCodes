package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class BreadthFirstSearch extends  
Vertex<IntWritable, IntWritable, NullWritable, IntWritable> {

	public static final String BFS_SOURCE = "bfs.sourceid";
	public static final int DEFAULT_SOURCE = 1;
	
	boolean visited = false;
	
	boolean isSource(){
		return getId().get() == 
				getContext().getConfiguration().getInt(BFS_SOURCE, DEFAULT_SOURCE);
	}
	
	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {
		if(!visited) {
			if(messages.iterator().hasNext() || isSource()){
				visited = true;
				sendMessageToAllEdges(new IntWritable(1));
			}
		}
		voteToHalt();
	}

}
