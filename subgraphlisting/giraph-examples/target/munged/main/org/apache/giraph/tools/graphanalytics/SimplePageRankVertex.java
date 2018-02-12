package org.apache.giraph.tools.graphanalytics;

//import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
public class SimplePageRankVertex extends 
	Vertex<IntWritable, DoubleWritable, NullWritable, DoubleWritable>{
  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 10;
  /** Logger */
  private static final Logger LOG = Logger.getLogger(SimplePageRankVertex.class);

  @Override
  public void compute(Iterable<DoubleWritable> messages) {
	if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
	 // LOG.info("*********************value="+getValue().get()+" sum="+sum+"************");
      DoubleWritable vertexValue =
          new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
      setValue(vertexValue);
    }

    if (getSuperstep() < MAX_SUPERSTEPS) {
      long edges = getNumEdges();
      sendMessageToAllEdges(
          new DoubleWritable(getValue().get() / edges));
    } else {
      voteToHalt();
    }
  }
  
  /** Vertex InputFormat */
  public static class SimplePageRankVertexInputFormat extends
  AdjacencyListTextVertexInputFormat<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {
	/** Separator for id and value */
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	@Override
	public AdjacencyListTextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) {
		return new SimplePageRankReader();
	}
	
	public class  SimplePageRankReader extends AdjacencyListTextVertexReader {
		
		protected String[] preprocessLine(Text line) throws IOException {
			String[] values = SEPARATOR.split(line.toString());
		/*	LOG.info("Input Line: "+ line.toString());
			int i;
			for(i = 0; i < values.length; i++) {
				LOG.info("\t"+values[i]);
			}*/
			return values;
		}

		    @Override
		    protected IntWritable getId(String[] values) throws IOException {
		      return decodeId(values[0]);
		    }


		    @Override
		    protected DoubleWritable getValue(String[] values) throws IOException {
		      return decodeValue(null);
		    }

		    @Override
		    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(String[] values) throws
		        IOException {
		      int i = 1;
		      List<Edge<IntWritable, NullWritable>> edges = Lists.newLinkedList();
		      while (i < values.length) {
		        edges.add(decodeEdge(values[i], null));
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
			return new DoubleWritable(1.0);
		}

		@Override
		public Edge<IntWritable, NullWritable> decodeEdge(String id,
				String value) {
			return EdgeFactory.create(decodeId(id), NullWritable.get());
		}
	}
  } 
  
  /**
   * Simple VertexOutputFormat that supports {@link SimplePageRankVertex}
   */
  public static class SimplePageRankVertexOutputFormat extends
      TextVertexOutputFormat<IntWritable, DoubleWritable, NullWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new SimplePageRankVertexWriter();
    }

    /**
     * Simple VertexWriter that supports {@link SimplePageRankVertex}
     */
    public class SimplePageRankVertexWriter extends TextVertexWriter {
      @Override
      public void writeVertex(
          Vertex<IntWritable, DoubleWritable, NullWritable, ?> vertex)
        throws IOException, InterruptedException {
        getRecordWriter().write(
            new Text(vertex.getId().toString()),
            new Text(vertex.getValue().toString()));
      }
    }
  }
}

