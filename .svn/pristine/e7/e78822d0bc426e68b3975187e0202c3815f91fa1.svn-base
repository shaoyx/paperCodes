package org.apache.giraph.tools.graphanalytics.simrank;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.tools.utils.HashMapWritable;

public class HashMapWritableAggregator extends BasicAggregator<HashMapWritable> {

	@Override
	public void aggregate(HashMapWritable value) {
		if(value.getKeyEntry() != null){
			for(int vid : value.getKeyEntry()){
				getAggregatedValue().add(vid, value.get(vid));
			}
		}
	}

	@Override
	public HashMapWritable createInitialValue() {
		return new HashMapWritable();
	}

}
