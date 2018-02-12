package com.graphtools.utils;

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
	
	public void copy(EdgeOrientation other){
		vertexIdIndexMap = (HashMap<Integer, Integer>) other.vertexIdIndexMap.clone();
		for(int i = 0; i < edgeSequenceMatrix[0].length; i++){
			for(int j = 0; j < edgeSequenceMatrix[i].length; j++){
				edgeSequenceMatrix[i][j] = other.getEdgeOrientation(i, j);
			}
		}
	}

	public int getVertexIndex(int vid){
		return this.vertexIdIndexMap.get(vid);
	}
	
	public void setVertexIdIndex(int vid, int index){
		vertexIdIndexMap.put(vid, index);
	}
	
	public byte getEdgeOrientation(int source, int target){
		return edgeSequenceMatrix[source][target];
	}
	
	public void setEdgeOrientation(int source, int target, byte value){
		if(edgeSequenceMatrix[source][target] == EDGE_ORIENTATION_UNDEFINED)
			edgeSequenceMatrix[source][target] = value;
	}
	
	public boolean checkValid() {
		int size = edgeSequenceMatrix.length;
		byte [][][] edgeSequenceMultiple = new byte[2][size][size];
		int index = 0;
		for(int i = 0; i < size; i++){
			for(int j = 0; j < size; j++){
				edgeSequenceMultiple[index][i][j] = edgeSequenceMatrix[i][j];
				edgeSequenceMultiple[1-index][i][j] = edgeSequenceMatrix[i][j];
			}
		}
		
		boolean isValid = true;
		boolean isChanged = true;
		
		/**
		 * check validation through transitive property.
		 */
		while(isChanged && isValid){
			isChanged = false;
			for(int i = 0; i < size && isValid; i++){
				for(int j = 0; j < size && isValid; j++){
						if(i == j) continue;
					for(int k = 0; k < size && isValid; k++){
						if(i == k || j == k) continue;
						if(edgeSequenceMultiple[index][i][k] == edgeSequenceMatrix[k][j]
								&& edgeSequenceMatrix[k][j] != EDGE_ORIENTATION_UNDEFINED){
							if(edgeSequenceMultiple[1-index][i][j] == EDGE_ORIENTATION_UNDEFINED){
								isChanged = true;
								edgeSequenceMultiple[1-index][i][j] = edgeSequenceMatrix[k][j];
							}
							else if(edgeSequenceMultiple[1-index][i][j] != edgeSequenceMatrix[k][j]){
								isValid = false;
								break;
							}
						}
					}
				}
			}
			
			for(int i = 0; i < size; i++){
				for(int j = 0; j < size;j++){
					edgeSequenceMultiple[index][i][j] = edgeSequenceMultiple[1-index][i][j];
				}
			}
			index = 1 - index;
		}
		return isValid;
	}
	
	public void calClosure(){
		int size = edgeSequenceMatrix.length;
		byte [][][] edgeSequenceMultiple = new byte[2][size][size];
		int index = 0;
		for(int i = 0; i < size; i++){
			for(int j = 0; j < size; j++){
				edgeSequenceMultiple[index][i][j] = edgeSequenceMatrix[i][j];
				edgeSequenceMultiple[1-index][i][j] = edgeSequenceMatrix[i][j];
			}
		}
		
//		boolean isValid = true;
		boolean isChanged = true;
		
		/**
		 * check validation through transitive property.
		 */
		while(isChanged){
			isChanged = false;
			for(int i = 0; i < size; i++){
				for(int j = 0; j < size; j++){
						if(i == j) continue;
					for(int k = 0; k < size; k++){
						if(i == k || j == k) continue;
						if(edgeSequenceMultiple[index][i][k] == edgeSequenceMatrix[k][j]
								&& edgeSequenceMatrix[k][j] != EDGE_ORIENTATION_UNDEFINED){
							if(edgeSequenceMultiple[1-index][i][j] == EDGE_ORIENTATION_UNDEFINED){
								isChanged = true;
								edgeSequenceMultiple[1-index][i][j] = edgeSequenceMatrix[k][j];
							}
							else if(edgeSequenceMultiple[1-index][i][j] != edgeSequenceMatrix[k][j]){
//								isValid = false;
								System.out.println("Error!! Invalid EO exist!");
								break;
							}
						}
					}
				}
			}
			
			for(int i = 0; i < size; i++){
				for(int j = 0; j < size;j++){
					edgeSequenceMultiple[index][i][j] = edgeSequenceMultiple[1-index][i][j];
				}
			}
			index = 1 - index;
		}
		
		for(int i = 0; i < size; i++){
			for(int j = 0;j < size; j++){
				edgeSequenceMatrix[i][j] = edgeSequenceMultiple[index][i][j];
			}
		}
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