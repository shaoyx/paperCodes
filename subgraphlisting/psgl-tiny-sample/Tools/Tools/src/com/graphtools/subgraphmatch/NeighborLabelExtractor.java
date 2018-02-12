package com.graphtools.subgraphmatch;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class NeighborLabelExtractor 
extends GraphLoader{

	private int labelNumber;
	private HashMap<Integer, ArrayList<Integer>> neighborLabels;
	
	public NeighborLabelExtractor(int labelNumber){
		this.labelNumber = labelNumber;
		neighborLabels = new HashMap<Integer, ArrayList<Integer>>();
	}
	
	public HashMap<Integer, ArrayList<Integer>> getNeighborLabel(){
		for(int vertexId : graph.keySet()){
			ArrayList<Integer> neighbors = graph.get(vertexId);
			int[] labelCount = new int[labelNumber+1];
			for(int adj : neighbors){
				labelCount[vertexLabelMap.get(adj)]++;
			}
			ArrayList<Integer> al = new ArrayList<Integer>();
			for(int i = 0; i <= labelNumber; i++){
				al.add(labelCount[i]);
			}
			neighborLabels.put(vertexId, al);
		}
		return neighborLabels;
	}
	
	public void saveNeighborLabels(String path){
		try{
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(path)));
			
			for(int vertexId : graph.keySet()){
				String ans = String.valueOf(vertexId) +" " + vertexLabelMap.get(vertexId)+" " + graph.get(vertexId).size();
				for(int labelCountIdx = 0; labelCountIdx <= labelNumber; labelCountIdx++){
					ans += " "+ neighborLabels.get(vertexId).get(labelCountIdx);
				}
				bw.write(ans);
				bw.newLine();
			}
			bw.close();
			
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
