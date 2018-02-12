package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class RandomWalksProbability 
implements WritableComparable {
	boolean isSrcProb;
	double prob;
	double dupProb; /* record the probability of src and dst having been met. */

	public RandomWalksProbability(){
	}
	
	public RandomWalksProbability(boolean isSrcProb, double prob, double dupProb){
		this.isSrcProb = isSrcProb;
		this.prob = prob;
		this.dupProb = dupProb;
	}
	
	public boolean isSrcProb(){
		return this.isSrcProb;
	}
	
	public double getProb(){
		return this.prob;
	}
	
	public void setProb(double prob){
		this.prob = prob;
	}
	
	public double getDupProb(){
		return this.dupProb;
	}
	
	public void setDupProb(double dupProb){
		this.dupProb = dupProb;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		isSrcProb = in.readBoolean();
		prob = in.readDouble();
		dupProb = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isSrcProb);
		out.writeDouble(prob);
		out.writeDouble(dupProb);
	}

	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
