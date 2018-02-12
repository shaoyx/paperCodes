package org.apache.giraph.tools.graphanalytics.relextractor;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

public class LabeledGraphVertexInputFormat extends
	AdjacencyListTextVertexInputFormat<IntWritable, IntWritable, IntWritable> {
	/** Separator for id and value */
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	@Override
	public AdjacencyListTextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) {
		return new LabeledGraphReader();
	}

	public class  LabeledGraphReader extends AdjacencyListTextVertexReader {

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
			return new IntWritable(Integer.valueOf(values[1])); //vertex label;
		}

		@Override
		protected Iterable<Edge<IntWritable, IntWritable>> getEdges(String[] values) throws
		IOException {
			int i = 2;
//			List<Edge<IntWritable, IntWritable>> inEdges = Lists.newLinkedList();
			List<Edge<IntWritable, IntWritable>> outEdges = Lists.newLinkedList();
			while (i < values.length) {
				int target = Integer.valueOf(values[i]);
				int elabel = Integer.valueOf(values[i+1]);
				String edirection = values[i+2];
				if("O".equals(edirection)) {
					outEdges.add(EdgeFactory.create(new IntWritable(target), new IntWritable(elabel)));
				}
				i += 3;
			}
			return outEdges;
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
		public Edge<IntWritable, IntWritable> decodeEdge(String id,
				String value) {
			return null;
		}
	}
}
