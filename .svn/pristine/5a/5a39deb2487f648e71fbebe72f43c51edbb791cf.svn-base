package org.apache.giraph.tools.graphanalytics.simrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

// implements WritableComparable
public class PathWritable implements WritableComparable{

	private int sid;
	private int[] path;
	private int tail = 0;
	private int size = 0;
	private int count;
	private boolean isFull;
	
	public PathWritable(){}
	
	public PathWritable(int sid, int pathLen){
		if(pathLen <= 0){
			count = pathLen;
			this.sid = sid;
		}
		else{
			count = 0;
			this.sid = sid;
			path = new int [pathLen];
			tail = 0;
			size = pathLen;
			isFull = false;
		}
	}
	
	public void addVertex(int vid, int leftSteps){
		path[tail] = vid;
		tail++;
		if(tail == size){
			isFull = true;
			tail = 0;
		}
		if(leftSteps > 0){
			count = leftSteps;
		}
	}
	
	public int getMeetPoints(short timestamp) {
		int idx  = tail - 1 - timestamp;
		if(isFull && tail < 0){
			idx += size;
		}
		return (idx < 0 ? -1 : path[idx]);
	}
	
	public boolean isFinished(){
		return (count == 0);
	}
	
	public boolean isQueryMessage(){
		return count < 0;
	}
	
	public int getSampleId(){
		return sid;
	}
	
	public void decCount(){
		count--;
	}
		
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		out.writeInt(sid);
		if(count >= 0){
			out.writeInt(size);
			out.writeBoolean(isFull);
			out.writeInt(tail);
			if(isFull){
				out.writeInt(path.length);
				for(int i = 0; i < path.length; i++){
					out.writeInt(path[i]);
				}
			}
			else{
				out.writeInt(tail);
				for(int i = 0; i < tail; i++){
					out.writeInt(path[i]);
				}
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		count = in.readInt();
		sid = in.readInt();
		if(count >= 0){
			size = in.readInt();
			isFull = in.readBoolean();
			tail = in.readInt();
			int len = in.readInt();
			path = new int[size];
			for(int i = 0; i < len; i++){
				path[i] = in.readInt();
			}
		}
	}

	@Override
	public int compareTo(Object arg) {
		PathWritable other = (PathWritable)arg;
		return this.count - other.count;
	}
}
