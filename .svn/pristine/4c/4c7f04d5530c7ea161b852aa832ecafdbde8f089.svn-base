package org.apache.giraph.subgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class RawEdge implements Writable{
	
	private static final Logger LOG = Logger.getLogger(RawEdge.class);
	
	public static final byte RAWEDGE_INTERNAL = 1;
	public static final byte RAWEDGE_EXTERNAL = -1;
	public static final byte RAWEDGE_CROSS = 0;
	
	private int first;
	private int second;
	private byte type;
	
	private boolean deleted;
	private int count;
	
	public RawEdge() { }
	
	public RawEdge(int first, int second, byte type){
		this.initialize(first, second);
		this.type = type;
	}
	
	public void initialize(int first, int second){
		this.first = (first < second) ? first : second;
		this.second = (first < second) ? second : first;
		deleted = false;
		count = 0;
	}
	
	public long getId(){
		return (first<<32L+second);
	}
	
	public int getSmallEndPoint(){
		return first; 
	}
	
	public int getBigEndPoint(){
		return second;
	}
	
	public int getCount(){
		return count;
	}
	
	public void incCount(){
		count++;
	}
	
	public byte getEdgeType(){
		return type;
	}
	
	public void setEdgeType(byte type){
		this.type = type;
	}
	
	public void delete(){
		deleted = true;
	}
	
	public boolean isInternal(){
		return (type == RAWEDGE_INTERNAL);
	}
	
	public boolean isExternal(){
		return type == RAWEDGE_EXTERNAL;
	}
	
	public boolean isCROSS(){
		return type == RAWEDGE_CROSS;
	}
	
	public boolean isDelete(){
		return deleted;
	}
	
	public static long constructEdgeId(long a, long b){
		long id = a < b ? ((a<<32L)+b) : ((b<<32L)+a);
//		LOG.info("Edge("+a+", "+b+")"+" ==> id="+id);
		return id;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int decAndGetCount() {
		count--;
		return count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
		out.writeByte(type);
//		out.writeBoolean(deleted);
//		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
		type = in.readByte();
		deleted = false;
		count = 0;
	}
	
	public String toString(){
		return "Edge "+RawEdge.constructEdgeId(first, second)+": ("+first+", "+second+")"+
	"type="+type+" delted="+deleted+" support="+count;
	}
}
