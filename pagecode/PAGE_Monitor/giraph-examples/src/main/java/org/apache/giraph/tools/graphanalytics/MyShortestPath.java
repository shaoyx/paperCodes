package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;


import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;

public class MyShortestPath extends 
	Vertex <LongWritable, DoubleWritable, DoubleWritable, DoubleWritable>{
	
	final int sourceId = 1;
	
	private final Logger LOG =
			Logger.getLogger(MyShortestPath.class);
	
	private boolean isSource() {
		return getId().get() == sourceId;
	}
	
	@Override
	public void compute(Iterable <DoubleWritable> messages) {
		if (getSuperstep() == 0) {
			setValue(new DoubleWritable(Double.MAX_VALUE));
		}
		
		double dist = isSource() ? 0 : Double.MAX_VALUE;
		for (DoubleWritable message : messages) {
			dist = Math.min(dist, message.get());
		}
		
	    if (LOG.isDebugEnabled()) {
	        LOG.debug("Vertex " + getId() + " got minDist = " + dist +
	            " vertex value = " + getValue());
	      }	
	    
	    if (dist < getValue().get()) {
	    	setValue(new DoubleWritable(dist));
	    	for (Edge <LongWritable, DoubleWritable> edge : getEdges()) {
	    		double distance = dist + edge.getValue().get();
	    		if (LOG.isDebugEnabled()) {
	    			LOG.debug("Vertex " + getId() + " sent to " + edge.getTargetVertexId() +
	    					' ' + distance + " distance.");
	    		}
	    		sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
	    	}
	    }
	    
	    voteToHalt();
	}
	
	public static class MyVertexImputFormat extends AdjacencyListTextVertexInputFormat 
	<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
		@Override
		public AdjacencyListTextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context) {
			return new MyReader();
		}
		
		public class MyReader extends AdjacencyListTextVertexReader {

			@Override
			public LongWritable decodeId(String s) {
				// TODO Auto-generated method stub
				return new LongWritable(Long.valueOf(s));
			}

			@Override
			public DoubleWritable decodeValue(String s) {
				// TODO Auto-generated method stub
				return new DoubleWritable(Double.valueOf(s));
			}

			@Override
			public Edge<LongWritable, DoubleWritable> decodeEdge(String id,
					String value) {
				// TODO Auto-generated method stub
				return EdgeFactory.create(decodeId(id), decodeValue(value));
			}
			
		}
		
	}
	
	public static class MyVertexOutputFormat extends TextVertexOutputFormat
		<LongWritable, DoubleWritable, DoubleWritable> {
	
		@Override
		public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
			return new MyWriter();
		}
		
		public class MyWriter extends TextVertexWriter {
			@Override
			public void writeVertex(Vertex<LongWritable, DoubleWritable, DoubleWritable, ?> vertex) throws IOException, InterruptedException {
				getRecordWriter().write(
						new Text(vertex.getId().toString()),
						new Text(vertex.getValue().toString()));
			}

		}
	}
}

