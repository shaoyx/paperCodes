package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class GeneralPartialQueryWritableMemOp 
implements Writable{
	/*
	 * -2^32 indicates the vertex is not matched 
	 */
	private final int PARTIAL_QUERY_NOT_MATCHED = 0x80000000;
	private final byte QUERY_SEQUENCE_TYPE_UNKNOWN = -1;
	
	/*
	 * we fix the order of query node in preprocess.
	 * the positive matched value indicates matched and accessed
	 * the negative matched value indicates matched, but not accessed
	 */
	private int[] dataGraphNode;
	private byte totalPairs;
	private byte remainEdges;
	
	/* indicate the sequence of a query. */
	private byte querySequenceType; 
	
	/* record previous matched vertex id. */
	private byte previousMatchedVertexIndex;

	public GeneralPartialQueryWritableMemOp(){
		totalPairs = 0;
		remainEdges = 0;
		querySequenceType = QUERY_SEQUENCE_TYPE_UNKNOWN;
		dataGraphNode = null;
	}
	
	/**
	 * undirected edges count once.
	 */
	public GeneralPartialQueryWritableMemOp(int queryNodeSize, int totalEdges){
		dataGraphNode = new int[queryNodeSize];
		totalPairs = (byte)queryNodeSize;
		
		this.initialize(queryNodeSize, totalEdges);		
//		remainEdges = (byte)totalEdges;
//		previousMatchedVertexIndex = -1;
//		querySequenceType = QUERY_SEQUENCE_TYPE_UNKNOWN;
//		for(int i = 0; i < totalPairs; i++){
//			dataGraphNode[i] = PARTIAL_QUERY_NOT_MATCHED;
//		}
	}
	
	public void initialize(int queryNodeSize, int totalEdges){
		remainEdges = (byte)totalEdges;
		previousMatchedVertexIndex = -1;
		querySequenceType = QUERY_SEQUENCE_TYPE_UNKNOWN;
		for(int i = 0; i < totalPairs; i++){
			dataGraphNode[i] = PARTIAL_QUERY_NOT_MATCHED;
		}
	}
	
	public void setQuerySequenceType(int querySequenceType){
		this.querySequenceType = (byte)querySequenceType;
	}
	
	public int getQuerySequenceType(){
		return this.querySequenceType;
	}
	
	/**
	 * The query is fixed when all the edges are accessed.
	 * @return
	 */
	public Boolean isCompelete(){
		return remainEdges == 0;
	}
	
	public void decRemainEdges(){
		remainEdges--;
	}
	
	public void decRemainEdges(int value){
		remainEdges -= value;
	}
	
	/**
	 * nodeIndex means the index in QueryGraph, not the accurate node id.
	 * @param nodeIndex
	 * @return
	 */
	public Boolean isMatched(int nodeIndex){
		return (dataGraphNode[nodeIndex] != PARTIAL_QUERY_NOT_MATCHED);
	}
	
	public Boolean isAccessed(int nodeIndex){
		return (dataGraphNode[nodeIndex] > 0);
	}
	
	public boolean checkValid(){
		for(int i = 0; i < totalPairs; i++){
			if(dataGraphNode[i] == PARTIAL_QUERY_NOT_MATCHED){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * nodeIndex means the index in QueryGraph, not the accurate node id.
	 * Before calling this method, we should make sure that the vertex is matched, i.e
	 * calling isMatched(nodeIndex) before calling this method. 
	 * In order to avoid vertexId is ZERO, we add a shift of 1. 
	 * @param nodeIndex
	 * @return
	 */
	public int getMappedDataGraphNode(int nodeIndex){
		return dataGraphNode[nodeIndex] > 0 ? (dataGraphNode[nodeIndex]-1) : (-dataGraphNode[nodeIndex]-1);
	}
	
	/**
	 * return the index of unaccessed query vertex
	 * @return
	 */
	public ArrayList<Integer> getUnaccessedList(){
		ArrayList<Integer> al = new ArrayList<Integer>();
		for(int i = 0; i < totalPairs; i++){
			if(dataGraphNode[i] < 0 && dataGraphNode[i] != PARTIAL_QUERY_NOT_MATCHED){
				al.add(i);
			}
		}
		return al;
	}

	public void update(int candIndex, int i) {
//		if(dataGraphNode[candIndex] == PARTIAL_QUERY_NOT_MATCHED)
//			currentPairs++;
		dataGraphNode[candIndex] = (i == PARTIAL_QUERY_NOT_MATCHED ? PARTIAL_QUERY_NOT_MATCHED : -(i+1));
	}

	public int getPreviousMatchedVertexIndex() {
		return previousMatchedVertexIndex;
	}
	
	public void setPreviousMatchedVertexIndex(int index){
		if(dataGraphNode[index] < 0){
			dataGraphNode[index] = -dataGraphNode[index];
		}
		else{
			System.out.println("Conflict in mapping for matched vertex!");
		}
		previousMatchedVertexIndex = (byte)index;
	}

	public boolean hasUsed(int dataVertexId) {
		int tmpTarget = dataVertexId + 1;
		for(int i = 0; i < totalPairs; i++){
			if(dataGraphNode[i] == -tmpTarget || dataGraphNode[i] == tmpTarget){
				return true;
			}
		}
		return false;
	}

	public int getRemainEdges() {
		return remainEdges;
	}
	
	public void copy(GeneralPartialQueryWritableMemOp pq) {
		dataGraphNode = pq.dataGraphNode.clone();
		totalPairs = pq.totalPairs;
		remainEdges = pq.remainEdges;
		querySequenceType = pq.querySequenceType;
		previousMatchedVertexIndex = pq.previousMatchedVertexIndex;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(totalPairs); //1B
		out.writeByte(remainEdges); //1B
		out.writeByte(querySequenceType); //1B
		out.writeByte(previousMatchedVertexIndex);//1B
		for(int i = 0; i < totalPairs; ++i){
			out.writeInt(dataGraphNode[i]); // totalPairs * 4B
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		totalPairs = in.readByte();
		remainEdges = in.readByte();
		querySequenceType = in.readByte();
		previousMatchedVertexIndex = in.readByte();
//		System.out.println("Incoming:\n"+totalPairs+" "+currentPairs+" "+previousMatchedVertexIndex+"\n");
		dataGraphNode = new int[totalPairs];
		for(int i = 0; i < totalPairs; ++i){
			dataGraphNode[i] = in.readInt();
		}
	}

	public String toString(){
		String ans = "totalPairs = " + totalPairs
				+"\nremainEdges= "+ remainEdges
				+"\nquerySequenceType= "+ querySequenceType
				+"\npreviousMatchedVertexIndex= "+ previousMatchedVertexIndex
				+"\n";
		
		for(int i = 0; i < totalPairs; i++){
			ans = ans + (dataGraphNode[i] > 0 ? (dataGraphNode[i]-1) : (dataGraphNode[i]+1))+" ";
		}
		
		return ans;
	}

}
