package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

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
	
	/** Vertex InputFormat */
	public static class BreadthFirstSearchInputFormat extends
		AdjacencyListTextVertexInputFormat<IntWritable, IntWritable, NullWritable, IntWritable> {
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
							i ++;
						}
						return edges;
					}

				@Override
					public IntWritable decodeId(String s) {
						return new IntWritable(Integer.valueOf(s));
					}

				@Override
					public IntWritable decodeValue(String s) {
						return new IntWritable(1);
					}

				@Override
					public Edge<IntWritable, NullWritable> decodeEdge(String id,
							String value) {
						return EdgeFactory.create(decodeId(id), NullWritable.get());
					}
			}
		} 

	/**
	 * Simple VertexOutputFormat that supports {@link BreadthFirstSearch}
	 */
	public static class BreadthFirstSearchOutputFormat extends
		TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
			@Override
				public TextVertexWriter createVertexWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
				return new BreadthFirstSearchWriter();
				}

			/**
			 * Simple VertexWriter that supports {@link BreadthFirstSearch}
			 */
			public class BreadthFirstSearchWriter extends TextVertexWriter {
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
