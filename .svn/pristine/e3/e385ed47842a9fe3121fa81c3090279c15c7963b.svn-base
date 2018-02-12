package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MeetPoint 
implements WritableComparable {
	private int vid;
	private int meetLevel;
	private double prob;
	
	public MeetPoint() {}
	public MeetPoint(int vid, double prob, int meetLevel){
		this.vid = vid;
		this.prob = prob;
		this.meetLevel = meetLevel;
	}
	
	public int getVid(){
		return this.vid;
	}
	
	public double getProb(){
		return this.prob;
	}
	
	public int getMeetLevel(){
		return meetLevel;
	}
	
	public void incProb(double delta){
		this.prob += delta;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		vid = in.readInt();
		prob = in.readDouble();
		meetLevel = in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(vid);
		out.writeDouble(prob);
		out.writeInt(meetLevel);
	}
	
	@Override
	public int compareTo(Object arg0) {
		return 0;
	}
	public void divide(double degree) {
		prob /= degree;
	}
}