package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class RandomWalksWithMeetPoints 
implements WritableComparable {
	boolean isSrcProb;
	double prob;
	int size;
//	long multiple;
	ArrayList<MeetPoint> meetPoints; /* large memory here. */

	public RandomWalksWithMeetPoints(){
	}
	
	public RandomWalksWithMeetPoints(boolean isSrcProb, double prob){
		this.isSrcProb = isSrcProb;
		this.prob = prob;
//		this.multiple = multiple;
		size = -1;
	}
	
	public boolean isSrcProb(){
		return this.isSrcProb;
	}
	
	public double getProb(){
		return this.prob;
	}
	
//	public long getMultiple(){
//		return this.multiple;
//	}
//	
//	public void setMultiple(long multiple){
//		this.multiple = multiple;
//	}
	
	public void setProb(double prob){
		this.prob = prob;
	}
	
	public void addMeetPoint(int vid, double prob, int meetLevel){
		if(size == -1){
			meetPoints = new ArrayList<MeetPoint>();
			size = 0;
		}
		++size;
		meetPoints.add(new MeetPoint(vid, prob, meetLevel));
	}
	
	public double getLastMeetPoint(RandomWalksWithMeetPoints other){
		if(size == -1){
			return 0;
		}
		for(int i = size - 1; i >= 0; --i){
			int idx = other.isMeetPoint(meetPoints.get(i).getVid(), meetPoints.get(i).getMeetLevel());
			if(idx != -1){
				return other.getMeetPoint(idx).getProb();
			}
		}
		return 0;
	}
	
	public int isMeetPoint(int vid, int meetLevel){
		for(int i = size - 1; i >= 0; --i){
			if(meetPoints.get(i).getVid() == vid
					&& meetPoints.get(i).getMeetLevel() == meetLevel){
				return i;
			}
		}
		return -1;
	}
	
	public MeetPoint getMeetPoint(int idx){
		return this.meetPoints.get(idx);
	}
	
	public boolean equal(Object obj){
		RandomWalksWithMeetPoints other = (RandomWalksWithMeetPoints)obj;
		if(this.isSrcProb != other.isSrcProb || size != other.size)
			return false;
		for(int i = 0; i < size; ++i){
			if(this.meetPoints.get(i).getVid() != other.meetPoints.get(i).getVid()
					|| this.meetPoints.get(i).getMeetLevel() != other.meetPoints.get(i).getMeetLevel())
				return false;
		}
		return true;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		isSrcProb = in.readBoolean();
		prob = in.readDouble();
//		multiple = in.readLong();
		size = in.readInt();
		meetPoints = null;
		if(size > 0){
			meetPoints = new ArrayList<MeetPoint>();
			for(int i = 0; i < size; ++i){
				MeetPoint mp = new MeetPoint();
				mp.readFields(in);
				meetPoints.add(mp);
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isSrcProb);
		out.writeDouble(prob);
//		out.writeLong(multiple);
		out.writeInt(size);
		if(size > 0){
			for(int i = 0; i < size; ++i){
				meetPoints.get(i).write(out);
			}
		}
	}

	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	public void merge(RandomWalksWithMeetPoints msg) {
		this.prob += msg.getProb();
		for(int i = 0; i < size; ++i){
			meetPoints.get(i).incProb(msg.getMeetPoint(i).getProb());
		}
	}

	public void updateProb(double degree) {
		this.prob /= degree;
		for(int i = 0; i < size; i++){
			meetPoints.get(i).divide(degree);
		}
		
	}
	public String toString(){
		String result = "isSrc="+this.isSrcProb+" prob="+this.prob+" size="+size+" [";
		for(int i = 0; i < size; i++){
			result += "("+meetPoints.get(i).getVid()+","+meetPoints.get(i).getProb()+","+meetPoints.get(i).getMeetLevel()+") ";
		}
		result += "]";
		return result;
	}

	public void copy(RandomWalksWithMeetPoints msg) {
		this.isSrcProb = msg.isSrcProb;
		this.prob = msg.prob;
		this.size = msg.size;
//		this.multiple = msg.multiple;
		if(size != -1){
			this.meetPoints = new ArrayList<MeetPoint>();
		}
		for(int i = 0; i < size; ++i){
			meetPoints.add(new MeetPoint(msg.getMeetPoint(i).getVid(), msg.getMeetPoint(i).getProb(), msg.getMeetPoint(i).getMeetLevel()));
		}
	}
}
