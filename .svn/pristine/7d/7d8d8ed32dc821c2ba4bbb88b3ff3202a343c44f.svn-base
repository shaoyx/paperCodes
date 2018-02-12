package org.apache.giraph.tools.graphanalytics.simrank;

import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.tools.utils.HashMapWritable;
import org.apache.hadoop.io.IntWritable;

public class ProbCombiner extends Combiner<IntWritable, HashMapWritable> {

	@Override
	public void combine(IntWritable vertexIndex,
			HashMapWritable originalMessage, HashMapWritable messageToCombine) {
		originalMessage.combine(messageToCombine);
	}

	@Override
	public HashMapWritable createInitialMessage() {
		return new HashMapWritable();
	}
	
}
