package org.apache.giraph.tools.graphanalytics.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ProbPairWritable implements Writable{
	private int vertex;
	private double prob;
	
	public ProbPairWritable(){
		
	}
	
	public ProbPairWritable(int vid, double sr){
		vertex  = vid;
		prob = sr;
	}
	
	public int getVertex(){
		return vertex;
	}
	
	public double getprob(){
		return prob;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		vertex = in.readInt();
		prob = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(vertex);
		out.writeDouble(prob);
	}
}
