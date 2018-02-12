package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BitMapWritable 
extends BitMap 
implements WritableComparable{

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		int size = wordsLength();
		out.writeInt(size);
		for(int i = 0; i < size; i++){
			out.writeLong(getWord(i));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int size = in.readInt();
		initialWords(size);
		setWordLength(size);
		for(int i = 0; i < size; i++){
			setWord(i, in.readLong());
		}
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}

}
