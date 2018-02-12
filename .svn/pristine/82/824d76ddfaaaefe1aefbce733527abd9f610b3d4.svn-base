package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable{

	private int first;
	private int second;
	
	public PairWritable(){}
	
	public PairWritable(int first, int second){
		this.first = first;
		this.second = second;
	}
	
	public int getFirst(){
		return this.first;
	}
	
	public int getSecond(){
		return this.second;
	}
	
	public void setFirst(int first){
		this.first = first;
	}
	
	public void setSecond(int second){
		this.second = second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	@Override
	public int compareTo(Object obj) {
		PairWritable other = (PairWritable)obj;
		if(other.first != first){
			return first - other.first;
		}
		return second - other.second;
	}

}
