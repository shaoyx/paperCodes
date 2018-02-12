package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class BipartiteMatching extends 
Vertex<IntWritable, IntWritable, NullWritable, IntWritable> {
	
	boolean isMatched = false;
	int matchId = -1;

	/**
	 * vertexValue = 0, when the vertex is left-side vertex;
	 * vertexValue = 1, when the vertex is right-side vertex;
	 */
	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {
		if(isMatched == true){
			voteToHalt();
			return;
		}
		int phase = (int)(getSuperstep() % 4);
		switch(phase){
		case 0:
			/* left-side vertex sends match request */
			if(getValue().get() == 0){
				sendMessageToAllEdges(getId());
			}
			break;
		case 1:
			/* right-side vertex random selects the matchId */
			if(getValue().get() == 1 && messages.iterator().hasNext()){
				int tmp = messages.iterator().next().get();
				sendMessage(new IntWritable(tmp), new IntWritable(tmp));
			}
			break;
		case 2:
			if(getValue().get() == 0 && messages.iterator().hasNext() && matchId == -1){
				matchId = messages.iterator().next().get();
				isMatched = true;
				sendMessage(new IntWritable(matchId), new IntWritable(matchId));
				voteToHalt();
			}
			break;
		case 3:
			if(getValue().get() == 1 && messages.iterator().hasNext() && matchId == -1){
				matchId = messages.iterator().next().get();
				isMatched = true;
				voteToHalt();
			}
			break;
		}
		
	}

}
