package org.apache.giraph.utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;

public class EdgeOrientation {
	
	public final static byte EDGE_ORIENTATION_BIG = 2;
	public final static byte EDGE_ORIENTATION_SMALL = 1;
	public final static byte EDGE_ORIENTATION_BOTH = 0;
	public final static byte EDGE_ORIENTATION_UNDEFINED = -1;
		
//	private int vertexSize;
	private HashMap<Integer, Integer> vertexIdIndexMap;
	private byte [][] edgeSequenceMatrix;
	
	public EdgeOrientation(int vertexSize){
//		this.vertexSize = vertexSize;
		vertexIdIndexMap = new HashMap<Integer, Integer>();
		edgeSequenceMatrix = new byte[vertexSize][vertexSize];
		for(int i = 0; i < vertexSize; i++){
			for(int j = 0; j < vertexSize; j++){
				edgeSequenceMatrix[i][j] = EDGE_ORIENTATION_UNDEFINED;
			}
		}
	}
	
	public EdgeOrientation() { };
	
	public byte getEdgeOrientation(int source, int target){
		return edgeSequenceMatrix[source][target];
	}
	
	public void setEdgeOrientation(int source, int target, byte value){
		edgeSequenceMatrix[source][target] = value;
	}	
	
	public int getVertexIndex(int vid){
		return this.vertexIdIndexMap.get(vid);
	}
	
	public void setVertexIdIndex(int vid, int index){
		vertexIdIndexMap.put(vid, index);
	}
	
	public String toString(){
		String result = "Size="+vertexIdIndexMap.size()+"\n";
		for(int vid : vertexIdIndexMap.keySet()){
			result += "\t"+vid + " ==> "+ vertexIdIndexMap.get(vid)+"\n";
		}
		for(int i = 0; i < vertexIdIndexMap.size(); i++){
			for(int j = 0; j < vertexIdIndexMap.size(); j++){
				if(edgeSequenceMatrix[i][j] == EDGE_ORIENTATION_UNDEFINED){
					result += "0 ";
				}
				else if(edgeSequenceMatrix[i][j] == EDGE_ORIENTATION_BIG){
					result += "> ";
				}
				else if(edgeSequenceMatrix[i][j] == EDGE_ORIENTATION_SMALL){
					result += "< ";
				}
				else{
					result += "= ";
				}
			}
			result +="\n";
		}
		return result;
	}
	
	public void write(DataOutputStream out){
		int size = vertexIdIndexMap.size();
		try {
			out.writeInt(size);
			for(int key : vertexIdIndexMap.keySet()){
				out.writeInt(key);
				out.writeInt(vertexIdIndexMap.get(key));
			}
			for(int i = 0; i < size; i++){
				for(int j = 0; j < size; j++){
					out.writeByte(edgeSequenceMatrix[i][j]);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void readFileds(DataInputStream in){
		try {
			int size = in.readInt();
			vertexIdIndexMap = new HashMap<Integer, Integer>();
			edgeSequenceMatrix = new byte[size][size];
			
			for(int i = 0; i < size; i++){
				int key = in.readInt();
				int value = in.readInt();
				vertexIdIndexMap.put(key, value);
			}
			for(int i = 0; i < size; i++){
				for(int j = 0; j < size; j++){
					edgeSequenceMatrix[i][j] = in.readByte();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
