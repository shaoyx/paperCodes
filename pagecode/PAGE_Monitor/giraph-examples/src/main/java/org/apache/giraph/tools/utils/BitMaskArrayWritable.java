package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;

public class BitMaskArrayWritable  implements
Writable{
	
//	private final static long v62 = 62;
	private long [] bitmask;
	private int bitmaskSize;

	public BitMaskArrayWritable(int bitMaskArraySize) {
		this.initialize(bitMaskArraySize);
	}
	
	public BitMaskArrayWritable(){
		bitmaskSize = 0;
	}
	
	public void initialize(int bitMaskArraySize) {
		bitmaskSize = bitMaskArraySize;
		if(bitmaskSize == 0) return;
		bitmask = new long[bitMaskArraySize];
		for(int i = 0; i < bitmaskSize; i++){
			long randVal = this.createRandomBitmask(63);
			bitmask[i] = (1L << (62L-randVal));
		}
	}
	
	private int createRandomBitmask(int bitmaskLength){
		int j;
		// cur_random is between 0 and 1.
		double cur_random = Math.random();
		double threshold = 0;
		for (j = 0; j < bitmaskLength - 1; j++) {
			threshold += Math.pow(2.0, -1 * j - 1);
			if (cur_random < threshold) {
				break;
			}
		}
		return j;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		this.bitmaskSize = in.readInt();
		if(bitmaskSize > 0){
			this.bitmask = new long[bitmaskSize];
			for(int i = 0; i < bitmaskSize; i++){
				bitmask[i] = in.readLong();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.bitmaskSize);
		for(int i = 0; i < bitmaskSize; i++){
			out.writeLong(bitmask[i]);
		}
	}

	public long getBitmask(int index) {
		return bitmask[index];
	}
	
	public void or(int index, long value){
		bitmask[index] |= value;
	}

//	@Override
//	public int compareTo(BitMaskArrayWritable obj) {
//		return 0;
//	}
}
