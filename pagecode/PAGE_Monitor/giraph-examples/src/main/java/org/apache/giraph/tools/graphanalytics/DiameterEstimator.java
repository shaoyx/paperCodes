package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.tools.utils.BitMaskArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;


public class DiameterEstimator extends
Vertex<IntWritable, BitMaskArrayWritable, NullWritable, BitMaskArrayWritable> {
	
	private int superstepLimitation;
	private int bitMaskArraySize;
	
	public void setup(){
		this.superstepLimitation = getConf().getInt("diameter.supersteplimitaion", 10);
		this.bitMaskArraySize = getConf().getInt("diameter.bitmaskarraysize", 16);
		
		BitMaskArrayWritable bmaValue = new BitMaskArrayWritable();
		bmaValue.initialize(this.bitMaskArraySize);
		this.setValue(bmaValue);
	}

	@Override
	public void compute(Iterable<BitMaskArrayWritable> messages)
			throws IOException {
		if(getSuperstep() == 0){
			setup();
		}
		
		if(getSuperstep() > superstepLimitation){
			voteToHalt();
			return;
		}
		
		/* do the computation */
		BitMaskArrayWritable bma = this.getValue();
		for(BitMaskArrayWritable msg : messages){
			for(int i = 0; i < bitMaskArraySize; i++){
				bma.or(i, msg.getBitmask(i));
			}
		}
		this.sendMessageToAllEdges(bma);
		this.setValue(bma);
	}
	
	
	/** Vertex InputFormat */
	public static class DiameterEstimatorInputFormat extends
		AdjacencyListTextVertexInputFormat<IntWritable, BitMaskArrayWritable, NullWritable, BitMaskArrayWritable> {
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
					protected BitMaskArrayWritable getValue(String[] values) throws IOException {
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
					public BitMaskArrayWritable decodeValue(String s) {
						return new BitMaskArrayWritable();
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
	public static class DiameterEstimatorOutputFormat extends
		TextVertexOutputFormat<IntWritable, BitMaskArrayWritable, NullWritable> {
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
							Vertex<IntWritable, BitMaskArrayWritable, NullWritable, ?> vertex)
					throws IOException, InterruptedException {
					getRecordWriter().write(
							new Text(vertex.getId().toString()),
							new Text(vertex.getValue().toString()));
							}
			}
		}
}
