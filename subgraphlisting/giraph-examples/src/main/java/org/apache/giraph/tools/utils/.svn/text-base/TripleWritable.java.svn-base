package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TripleWritable implements WritableComparable{

	private int first;
	private int second;
	private int third;
	
	public TripleWritable(){}
	
	public TripleWritable(int first, int second, int third){
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	public void initialize(int first, int second, int third){
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	public int getFirst(){
		return this.first;
	}
	
	public int getSecond(){
		return this.second;
	}
	
	public int getThird(){
		return this.third;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
		out.writeInt(third);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
		third = in.readInt();
	}

	@Override
	public int compareTo(Object obj) {
		TripleWritable other = (TripleWritable)obj;
		if(other.first != first){
			return first - other.first;
		}
		if(other.second != second){
			return second - other.second;
		}
		return third - other.third;
	}
	
	public String toString(){
		return "( "+first+", "+second+", "+third+" )";
	}
}
