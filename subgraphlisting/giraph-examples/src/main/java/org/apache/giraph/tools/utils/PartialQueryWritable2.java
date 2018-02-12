package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.WritableComparable;

public class PartialQueryWritable2
implements WritableComparable{
		
		public static final int PARTIAL_QUERY_MAX_NODE_SIZE = 25; 
		public static final int PARTIAL_QUERY_NOT_MATCHED = -1;
		
		private int[] dataGraphNode;
		private int totalPairs;
//		private int currentPairs; /* used for debug: check the query is compleleted.*/
		private int remainUnAccessedEdge;
		/*record previous matched vertex id.*/
		private int previousMatchedVertexIndex;
		private HashMap<Integer, BitMapWritable> candidatesList;
		
		public PartialQueryWritable2(){
			//this(PARTIAL_QUERY_MAX_NODE_SIZE);
			totalPairs = 0;
//			currentPairs = 0;
			remainUnAccessedEdge = 0;
			dataGraphNode = null;
			candidatesList = null;
		}
		
		public PartialQueryWritable2(int queryNodeSize, int totalEdges){
			dataGraphNode = new int[queryNodeSize];
			candidatesList = new HashMap<Integer, BitMapWritable>();
			totalPairs = queryNodeSize;
//			currentPairs = 0;
			remainUnAccessedEdge = totalEdges;
			previousMatchedVertexIndex = -1;
			for(int i = 0; i < totalPairs; i++){
				dataGraphNode[i] = PARTIAL_QUERY_NOT_MATCHED;
			}
		}
		
		public Boolean isCompelete(){
			return remainUnAccessedEdge == 0;
		}
		
		public int getUnMatched(){
			for(int i = 0; i < totalPairs; ++i){
				if(dataGraphNode[i] == PARTIAL_QUERY_NOT_MATCHED){
					return i;
				}
			}
			return -1;
		}
		
		/**
		 * nodeIndex means the index in QueryGraph, not the accurate node id.
		 * @param nodeIndex
		 * @return
		 */
		public Boolean isMatched(int nodeIndex){
			return (dataGraphNode[nodeIndex] != PARTIAL_QUERY_NOT_MATCHED);
		}
		
		/**
		 * nodeIndex means the index in QueryGraph, not the accurate node id.
		 * @param nodeIndex
		 * @return
		 */
		public int getMappedDataGraphNode(int nodeIndex){
			return dataGraphNode[nodeIndex];
		}

		public void update(int candIndex, int i) {
			// TODO Auto-generated method stub
//			if(dataGraphNode[candIndex] == PARTIAL_QUERY_NOT_MATCHED)
//				currentPairs++;
			dataGraphNode[candIndex] = i;
		}
		
		public void decRemainUnAccessedEdge(){
			remainUnAccessedEdge--;
		}

		public int getPreviousMatchedVertexIndex() {
			// TODO Auto-generated method stub
			return previousMatchedVertexIndex;
		}
		
		public void setPreviousMatchedVertexIndex(int index){
			previousMatchedVertexIndex = index;
		}

		public boolean hasUsed(int dataVertexId) {
			for(int i = 0; i < totalPairs; i++){
				if(dataGraphNode[i] == dataVertexId){
					return true;
				}
			}
			return false;
		}

		public int getTotalCombination() {
			int result = 1;
			for(BitMapWritable key : candidatesList.values()){
				result *= key.cardinality();
			}
			return result;
		}
		
		public BitMapWritable getCandidate(int query_node_index){
			return candidatesList.get(query_node_index);
		}
		
		public void updateCandidate(int query_node_index, BitMapWritable bmw){
			candidatesList.put(query_node_index, bmw);
		}


		public void removeCandidate(int nodeIndex) {
			candidatesList.remove(nodeIndex);
			
		}
		
		public void copy(PartialQueryWritable2 pq) {
			dataGraphNode = pq.dataGraphNode.clone();
			totalPairs = pq.totalPairs;
//			currentPairs = pq.currentPairs;
			remainUnAccessedEdge = pq.remainUnAccessedEdge;
			previousMatchedVertexIndex = pq.previousMatchedVertexIndex;
			candidatesList = (HashMap<Integer, BitMapWritable>) pq.candidatesList.clone();
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
//			System.out.println("output: " + this.toString());
			out.writeInt(totalPairs);
//			out.writeInt(currentPairs);
			out.writeInt(remainUnAccessedEdge);
			out.writeInt(previousMatchedVertexIndex);
			for(int i = 0; i < totalPairs; ++i){
				//out.write(queryGraphNode[i]);
				out.writeInt(dataGraphNode[i]);
			}
			out.writeInt(candidatesList.size());
			for(int key : candidatesList.keySet()){
				out.writeInt(key);
				candidatesList.get(key).write(out);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			totalPairs = in.readInt();
//			currentPairs = in.readInt();
			remainUnAccessedEdge = in.readInt();
			previousMatchedVertexIndex = in.readInt();
//			System.out.println("Incoming:\n"+totalPairs+" "+currentPairs+" "+previousMatchedVertexIndex+"\n");
			//queryGraphNode = new int[totalPairs];
			dataGraphNode = new int[totalPairs];
			for(int i = 0; i < totalPairs; ++i){
				dataGraphNode[i] = in.readInt();
			}
			int size = in.readInt();
			candidatesList = new HashMap<Integer, BitMapWritable>();
			for(int i = 0; i < size; i++){
				int key = in.readInt();
				BitMapWritable bmw = new BitMapWritable();
				bmw.readFields(in);
				candidatesList.put(key, bmw);
			}
		}

		@Override
		public int compareTo(Object arg0) {
			// TODO Auto-generated method stub
			return 0;
		}

		public String toString(){
			String ans = "totalPairs = " +totalPairs+"\nremainUnAccessedEdge= "+remainUnAccessedEdge+
					"\npreviousMatchedVertexIndex= "+previousMatchedVertexIndex+"\n";
			for(int i = 0; i < totalPairs; i++){
				ans = ans + dataGraphNode[i]+" ";
			}
			ans += "\ncandidatesList:\n";
			for(int key : candidatesList.keySet()){
				ans += "key="+key+": ";
				BitMapWritable bmw = candidatesList.get(key);
				ans += bmw.toString()+"\n";
			}
			return ans;
		}
}
