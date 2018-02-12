package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * Demonstrates the basic Pregel Single Source Shortest Path implementation.
 */
@Algorithm(
    name = "SSSP"
)

public class SingleSourceShortestPathVertex extends 
	Vertex<IntWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
	  /** The shortest paths id */
	  public static final String SOURCE_ID = "sssp.sourceid";
	  /** Default shortest paths id */
	  public static final int SOURCE_ID_DEFAULT = 1;
	  /** Class logger */
	  private static final Logger LOG =
	      Logger.getLogger(SingleSourceShortestPathVertex.class);

	  /**
	   * Is this vertex the source id?
	   *
	   * @return True if the source id
	   */
	  private boolean isSource() {
	    return getId().get() ==
	        getContext().getConfiguration().getInt(SOURCE_ID,
	            SOURCE_ID_DEFAULT);
	  }

	  @Override
	  public void compute(Iterable<DoubleWritable> messages) {
	    if (getSuperstep() == 0) {
	      setValue(new DoubleWritable(Double.MAX_VALUE));
	    }
	    double minDist = isSource() ? 0d : Double.MAX_VALUE;
	    for (DoubleWritable message : messages) {
	      minDist = Math.min(minDist, message.get());
	    }
	    if (LOG.isDebugEnabled()) {
	      LOG.debug("Vertex " + getId() + " got minDist = " + minDist +
	          " vertex value = " + getValue());
	    }
	    if (minDist < getValue().get()) {
	      setValue(new DoubleWritable(minDist));
	      for (Edge<IntWritable, DoubleWritable> edge : getEdges()) {
	        double distance = minDist + edge.getValue().get();
	        if (LOG.isDebugEnabled()) {
	          LOG.debug("Vertex " + getId() + " sent to " +
	              edge.getTargetVertexId() + " = " + distance);
	        }
	        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
	      }
	    }
	    voteToHalt();
	  }
	  
	  /** Vertex InputFormat */
	  public static class SingleSourceShortestPathVertexInputFormat extends
	  AdjacencyListTextVertexInputFormat<IntWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
		/** Separator for id and value */
		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

		@Override
		public AdjacencyListTextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context) {
			return new SSSPReader();
		}
		
		public class  SSSPReader extends AdjacencyListTextVertexReader {
			
			protected String[] preprocessLine(Text line) throws IOException {
				String[] values = SEPARATOR.split(line.toString());
				return values;
			}

			    @Override
			    protected IntWritable getId(String[] values) throws IOException {
			      return decodeId(values[0]);
			    }

			    @Override
			    protected DoubleWritable getValue(String[] values) throws IOException {
			      return new DoubleWritable(Double.MAX_VALUE);
			    }

			    @Override
			    protected Iterable<Edge<IntWritable, DoubleWritable>> getEdges(String[] values) throws
			        IOException {
			      int i = 1;
			      List<Edge<IntWritable, DoubleWritable>> edges = Lists.newLinkedList();
			      while (i < values.length) {
			        edges.add(decodeEdge(values[i], "1.0"));
			        i ++;
			      }
			      return edges;
			    }
			
			@Override
			public IntWritable decodeId(String s) {
				return new IntWritable(Integer.valueOf(s));
			}

			@Override
			public DoubleWritable decodeValue(String s) {
				return new DoubleWritable(Double.valueOf(s));
			}

			@Override
			public Edge<IntWritable, DoubleWritable> decodeEdge(String id,
					String value) {
				return EdgeFactory.create(decodeId(id), decodeValue(value));
			}
		}
	  }
	  
	  /**
	   * SSSPVertexOutputFormat that supports {@link SingleSourceShortestPathVertex}
	   */
	  public static class SingleSourceShortestPathVertexOutputFormat extends
	      TextVertexOutputFormat<IntWritable, DoubleWritable, DoubleWritable> {
	    @Override
	    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return new SSSPWriter();
	    }

	    /**
	     * Simple VertexWriter that supports {@link SingleSourceShortestPathVertex}
	     */
	    public class SSSPWriter extends TextVertexWriter {
	      @Override
	      public void writeVertex(
	          Vertex<IntWritable, DoubleWritable, DoubleWritable, ?> vertex)
	        throws IOException, InterruptedException {
	        getRecordWriter().write(
	            new Text(vertex.getId().toString()),
	            new Text(vertex.getValue().toString()));
	      }
	    }
	  }
}
