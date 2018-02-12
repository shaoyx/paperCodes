package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class TwoHopNeighborSize extends 
org.apache.giraph.graph.Vertex<IntWritable, IntWritable, NullWritable, IntWritable>{
	  /** Logger */
	  private static final Logger LOG =
	      Logger.getLogger(TwoHopNeighborSize.class);
	  
	  /**
	   * Triangle Counting algorithm:
	   * Step 1: node u sends its neighbor size to each of its neighbor
	   * Step 2: node u aggregate the one-hop neighbor size.  
	   */
	@Override
	  public void compute(Iterable<IntWritable> messages) {
		  if(getSuperstep() == 0) {
			  sendMessageToAllEdges(new IntWritable(this.getNumEdges()));
		  }
		  else if(getSuperstep() == 1) {
			  int count = 0;
			  int incomingsize = 0;
			  for (IntWritable message : messages) {
				  count += message.get();
				  incomingsize++;
			  }
			  if(incomingsize != getNumEdges()){
				  LOG.info("incoming message size is not euqal to edgenumber!!!!!");
			  }
		      /*set to the vertex*/
		      this.setValue(new IntWritable((count + incomingsize)/incomingsize));
		      voteToHalt();			  
		  }
	  }
	  
	  /** Vertex InputFormat */
	  public static class TwoHopNeighborSizeInputFormat extends
	  AdjacencyListTextVertexInputFormat<IntWritable, IntWritable, NullWritable, IntWritable> {
		/** Separator for id and value */
		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

		@Override
		public AdjacencyListTextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context) {
			return new TwoHopNeighborSizeReader();
		}
		
		public class  TwoHopNeighborSizeReader extends AdjacencyListTextVertexReader {
			
			protected String[] preprocessLine(Text line) throws IOException {
				String[] values = SEPARATOR.split(line.toString());
				return values;
			}

			    @Override
			    protected IntWritable getId(String[] values) throws IOException {
			      return decodeId(values[0]);
			    }


			    @Override
			    protected IntWritable getValue(String[] values) throws IOException {
			      return decodeValue(null);
			    }

			    @Override
			    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(String[] values) throws
			        IOException {
			      int i = 1;
			      List<Edge<IntWritable, NullWritable>> edges = Lists.newLinkedList();
			      while (i < values.length) {
			        edges.add(decodeEdge(values[i], null));
			        i++;
			      }
			      return edges;
			    }
			
			@Override
			public IntWritable decodeId(String s) {
				return new IntWritable(Integer.valueOf(s));
			}

			@Override
			public IntWritable decodeValue(String s) {
				return new IntWritable(0);
			}

			@Override
			public Edge<IntWritable, NullWritable> decodeEdge(String id,
					String value) {
				return EdgeFactory.create(decodeId(id), NullWritable.get());
			}
		}
	  }
	  
	  public static class TwoHopNeighborSizeOutputFormat extends
	      TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
	    @Override
	    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return new TwoHopNeighborSizeWriter();
	    }
	    
	    public class TwoHopNeighborSizeWriter extends TextVertexWriter {
	      @Override
	      public void writeVertex(
	          Vertex<IntWritable, IntWritable, NullWritable, ?> vertex)
	        throws IOException, InterruptedException {
	        getRecordWriter().write(
	            new Text(vertex.getId().toString()),
	            new Text(vertex.getValue().toString()));
	      }
	    }
	  }
}
