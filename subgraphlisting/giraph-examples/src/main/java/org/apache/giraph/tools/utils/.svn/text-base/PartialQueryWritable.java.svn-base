package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.HashMap;

import org.apache.hadoop.io.Writable;

public class PartialQueryWritable 
implements Writable{
	/*
	 * -2^32 indicates the vertex is not matched 
	 */
	public static final int PARTIAL_QUERY_NOT_MATCHED = 0x80000000;
	
	/*
	 * we fix the order of query node in preprocess.
	 * the positive matched value indicates matched and accessed
	 * the negative matched value indicates matched, but not accessed
	 */
	private int[] dataGraphNode;
	private int totalPairs;
//	private int currentPairs;
	private int remainEdges;
	
	/*record previous matched vertex id.*/
	private int previousMatchedVertexIndex;

	public PartialQueryWritable(){
		totalPairs = 0;
//		currentPairs = 0;
		remainEdges = 0;
		dataGraphNode = null;
	}
	
	/**
	 * undirected edges count once.
	 */
	public PartialQueryWritable(int queryNodeSize, int totalEdges){
		//queryGraphNode = new int[queryNodeSize];
		dataGraphNode = new int[queryNodeSize];
		totalPairs = queryNodeSize;
//		currentPairs = 0;
		remainEdges = totalEdges;
		previousMatchedVertexIndex = -1;
		for(int i = 0; i < totalPairs; i++){
			dataGraphNode[i] = PARTIAL_QUERY_NOT_MATCHED;
		}
	}
	
	/**
	 * The query is fixed when all the edges are accessed.
	 * @return
	 */
	public Boolean isCompelete(){
//		return totalPairs == currentPairs;
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
	 * return the index list of unmatched query vertex
	 * @return
	 */
//	public ArrayList<Integer> getUnmatchedList(){
//		ArrayList<Integer> al = new ArrayList<Integer>();
//		for(int i = 0; i < totalPairs; i++){
//			if(dataGraphNode[i] == PARTIAL_QUERY_NOT_MATCHED){
//				al.add(i);
//			}
//		}
//		return al;
//	}
	
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
		dataGraphNode[candIndex] = -(i+1);
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
		previousMatchedVertexIndex = index;
	}
	
//	public void setAccessVertex(int index){		
//		if(dataGraphNode[index] < 0){
//			dataGraphNode[index] = -dataGraphNode[index];
//		}
//		else{
//			System.out.println("Conflict in mapping for matched vertex!");
//		}
//	}

	public boolean hasUsed(int dataVertexId) {
		int tmpTarget = dataVertexId + 1;
		for(int i = 0; i < totalPairs; i++){
			if(dataGraphNode[i] == -tmpTarget || dataGraphNode[i] == tmpTarget){
				return true;
			}
		}
		return false;
	}

	public void copy(PartialQueryWritable pq) {
		dataGraphNode = pq.dataGraphNode.clone();
		totalPairs = pq.totalPairs;
//		currentPairs = pq.currentPairs;
		remainEdges = pq.remainEdges;
		previousMatchedVertexIndex = pq.previousMatchedVertexIndex;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(totalPairs);
//		out.writeInt(currentPairs);
		out.writeInt(remainEdges);
		out.writeInt(previousMatchedVertexIndex);
		for(int i = 0; i < totalPairs; ++i){
			//out.write(queryGraphNode[i]);
			out.writeInt(dataGraphNode[i]);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		totalPairs = in.readInt();
//		currentPairs = in.readInt();
		remainEdges = in.readInt();
		previousMatchedVertexIndex = in.readInt();
//		System.out.println("Incoming:\n"+totalPairs+" "+currentPairs+" "+previousMatchedVertexIndex+"\n");
		//queryGraphNode = new int[totalPairs];
		dataGraphNode = new int[totalPairs];
		for(int i = 0; i < totalPairs; ++i){
			dataGraphNode[i] = in.readInt();
		}
	}

	public String toString(){
		String ans = "totalPairs = " +totalPairs+"\nremainEdges= "+remainEdges+
				"\npreviousMatchedVertexIndex= "+previousMatchedVertexIndex+"\n";
		for(int i = 0; i < totalPairs; i++){
			ans = ans + (dataGraphNode[i] > 0 ? (dataGraphNode[i]-1) : (dataGraphNode[i]+1))+" ";
		}
		return ans;
	}

	public int getRemainEdges() {
		return remainEdges;
	}

}
