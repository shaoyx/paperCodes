package com.graphtools.subgraphmatch;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

public class GraphLoader {
	protected static final Pattern SEPERATOR =  Pattern.compile("[\t ]");
	
	protected HashMap<Integer, ArrayList<Integer>> graph;
	protected HashMap<Integer, Integer> vertexLabelMap;
	protected ArrayList<Integer> vertexList;
	protected boolean isLoadGraph = false;
	protected boolean isLoadLabel = false;
	

	public void loadGraph(String graphFilePath){
		try {
			FileInputStream fin = new FileInputStream(graphFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			String line;
			graph = new HashMap<Integer, ArrayList<Integer>>();
			vertexList = new ArrayList<Integer>();
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				int vid = Integer.valueOf(values[0]);
				ArrayList<Integer> al = new ArrayList<Integer>();
				for(int i = 1; i < values.length; ++i){
					al.add(Integer.valueOf(values[i]));
				}
				vertexList.add(vid);
				graph.put(vid, al);
			}
			isLoadGraph = true;
			fbr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void loadLabel(String labelFilePath){
		try {
			FileInputStream fin = new FileInputStream(labelFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			String line;
			vertexLabelMap = new HashMap<Integer, Integer>();
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				vertexLabelMap.put(Integer.valueOf(values[0]), Integer.valueOf(values[1]));
			}
			isLoadLabel = true;
			fbr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
