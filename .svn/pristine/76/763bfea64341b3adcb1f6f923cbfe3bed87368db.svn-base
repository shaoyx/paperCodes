package org.apache.giraph.subgraph;


public class BasicEdge 
implements Comparable{
	
//	private static final Logger LOG = Logger.getLogger(BasicEdge.class);
	
	public static final byte BASICEDGE_INTERNAL = 1;
	public static final byte BASICEDGE_EXTERNAL = -1;
	public static final byte BASICEDGE_CROSS = 0;
	
	private int sourceId;
	private int targetId;
	private byte type;
	
	private boolean isMainCopy;
	private int count; /* supported value */
	
	public BasicEdge() { }
	
	public BasicEdge(int sourceId, int targetId, byte type){
		this.initialize(sourceId, targetId, type, false);
	}
	
	public BasicEdge(int sourceId, int targetId, byte type, boolean isMainCopy){
		this.initialize(sourceId, targetId, type, isMainCopy);
	}
	
	public void initialize(int sourceId, int targetId, byte type, boolean isMainCopy){
		this.sourceId = sourceId;
		this.targetId = targetId;
		this.type = type;
		this.isMainCopy = isMainCopy;
		count = 0;
	}
	
	public int getSourceId(){
		return this.sourceId;
	}
	
	public void setSourceId(int vid){
		this.sourceId = vid;
	}
	
	public int getTargetId(){
		return targetId;
	}
	
	public void setTargetId(int vid) {
		this.targetId = vid;
	}
	
	public boolean isMainCopy(){
		return this.isMainCopy;
	}
	
	public void setMainCopy(boolean value){
		this.isMainCopy = value;
	}
	
	public int getCount(){
		return count;
	}
	
	public void setCount(int count) {
		this.count = count;
	}

	public int decAndGetCount() {
		count--;
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
	
	public boolean isInternal(){
		return (type == BASICEDGE_INTERNAL);
	}
	
	public boolean isExternal(){
		return type == BASICEDGE_EXTERNAL;
	}
	
	public boolean isCROSS(){
		return type == BASICEDGE_CROSS;
	}

//	@Override
//	public void write(DataOutput out) throws IOException {
//		out.writeInt(first);
//		out.writeInt(second);
//		out.writeByte(type);
////		out.writeBoolean(deleted);
////		out.writeInt(count);
//	}
//
//	@Override
//	public void readFields(DataInput in) throws IOException {
//		first = in.readInt();
//		second = in.readInt();
//		type = in.readByte();
//		deleted = false;
//		count = 0;
//	}
	
	public String toString(){
		return "Edge "+BasicEdge.constructEdgeId(sourceId, targetId)+": ("+sourceId+", "+targetId+")"+
	"type="+type+" isMainCopy="+isMainCopy+" support="+count;
	}

	/**
	 * Treat it as undirected edge.
	 */
	public boolean equals(Object obj){
		BasicEdge other = (BasicEdge)obj;
		return (targetId == other.targetId && sourceId == other.sourceId) ||
				(targetId == other.sourceId && sourceId == other.targetId);
	}
	
	@Override
	public int compareTo(Object obj) {
		BasicEdge other = (BasicEdge)obj;
		if(sourceId != other.sourceId)
			return sourceId - other.sourceId;
		return targetId - other.targetId;
	}

	public static long constructEdgeId(long a, long b){
		long id = a < b ? ((a<<32L)+b) : ((b<<32L)+a);
//		LOG.info("Edge("+a+", "+b+")"+" ==> id="+id);
		return id;
	}

}
