package org.apache.giraph.tools.graphanalytics.relextractor;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LabeledGraphVertexOutputFormat extends
TextVertexOutputFormat<IntWritable, IntWritable, IntWritable> {
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	throws IOException, InterruptedException {
	return new LabeledGraphWriter();
	}
	
	public class LabeledGraphWriter extends TextVertexWriter {
		@Override
		public void writeVertex(
				Vertex<IntWritable, IntWritable, IntWritable, ?> vertex)
		throws IOException, InterruptedException {
			StringBuffer neighborList = new StringBuffer();
			int neighborCnt = 0;
			for(Edge<IntWritable, IntWritable> edge : vertex.getEdges()) {
				neighborCnt++;
				neighborList.append(" "+edge.getTargetVertexId()+" "+edge.getValue());
			}
			getRecordWriter().write(
				new Text(vertex.getId().toString()),
				new Text(neighborList.toString()));
		}
	}

}
