package org.apache.giraph.subgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class RawVertex implements Writable{
	
	private static final Logger LOG = Logger.getLogger(RawVertex.class);
	
	private boolean isLocal = false;
	private int id;
	private ArrayList<Integer> localNeighbors;
	private ArrayList<Integer> remoteNeighbors;
	private ArrayList<Integer> neighbors;
	
	public RawVertex() { }
	
	public void initialize(int id, boolean locality){
		this.id = id;
		isLocal = locality;
		neighbors = new ArrayList<Integer>();
	}
	
	public void setLocality(boolean locality){
		isLocal = locality;
	}
	
	public void addNeighbor(int id){
//		LOG.info("add neighbor=("+this.id+", "+id+")");
		neighbors.add(id);
	}
	
	public boolean isLocal(){
		return isLocal;
	}
	
	public int getId(){
		return id;
	}
	
	public ArrayList<Integer> getNeighbors(){
		return neighbors;
	}
	
	public boolean containNeighbor(int vid){
		return neighbors.contains(vid);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeBoolean(isLocal);
		out.writeInt(this.neighbors.size());
		for(int nvid : this.neighbors){
			out.writeInt(nvid);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		isLocal = in.readBoolean();
		int size = in.readInt();
		neighbors = new ArrayList<Integer>();
		for(int i = 0; i < size; i++){
			neighbors.add(in.readInt());
		}
	}
	
	public String toString(){
		String res = "vid="+id+" local="+isLocal+" nsize="+neighbors.size()+" nlist=(";
		for(int vid : neighbors){
			res += " "+vid;
		}
		res += ")";
		return res;
	}
}
