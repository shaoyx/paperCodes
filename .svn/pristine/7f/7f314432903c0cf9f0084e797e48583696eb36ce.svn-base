package org.apache.giraph.subgraph;

import java.util.Collection;
import java.util.HashMap;

import org.apache.log4j.Logger;

/**
 * The vertex stores its own neighborhood
 * @author simon0227
 *
 */
public class BasicVertex {
	
//	private static final Logger LOG = Logger.getLogger(BasicVertex.class);
//	private final BasicEdge compareStub = new BasicEdge(); 
	
	private int id;
	private HashMap<Integer, BasicEdge> neighbors;
	
	public BasicVertex() { }
	
	public void initialize(int id){
		this.id = id;
		neighbors = new HashMap<Integer, BasicEdge>();
//		compareStub.setSourceId(id);
	}
	
	public void addNeighbor(BasicEdge newEdge){
//		LOG.info("add neighbor=("+this.id+", "+id+")");
		if(neighbors.put(newEdge.getTargetId(), newEdge) != null){
			System.err.println("Changed the original existed edge");
		}
	}
	
	public int getId(){
		return id;
	}
	
	public Collection<BasicEdge> getNeighbors(){
		return neighbors.values();
	}
	
	public BasicEdge getNeighbor(int targetId){
		return neighbors.get(targetId);
	}
	
	public int getDegree(){
		return neighbors.size();
	}
	
	public boolean containNeighbor(int vid){
		return neighbors.containsKey(vid);
	}
	
	public boolean containNeighbor(BasicEdge ne){
		return neighbors.containsValue(ne);
	}
	
	public void removeEdge(int targetId) {
		neighbors.remove(targetId);
	}


//	@Override
//	public void write(DataOutput out) throws IOException {
//		out.writeInt(id);
//		out.writeBoolean(isLocal);
//		out.writeInt(this.neighbors.size());
//		for(int nvid : this.neighbors){
//			out.writeInt(nvid);
//		}
//	}
//
//	@Override
//	public void readFields(DataInput in) throws IOException {
//		id = in.readInt();
//		isLocal = in.readBoolean();
//		int size = in.readInt();
//		neighbors = new ArrayList<Integer>();
//		for(int i = 0; i < size; i++){
//			neighbors.add(in.readInt());
//		}
//	}
	
	public String toString(){
		String res = "vid="+id+" nsize="+neighbors.size()+" nlist=(";
		for(BasicEdge vid : neighbors.values()){
			res += " "+vid.getTargetId();
		}
		res += ")";
		return res; //res;
	}

}
