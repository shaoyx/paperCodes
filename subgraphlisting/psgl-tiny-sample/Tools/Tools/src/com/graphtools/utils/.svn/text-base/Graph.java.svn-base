package com.graphtools.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class Graph {
	protected static final Pattern SEPERATOR =  Pattern.compile("[\t ]");

	private Set<Integer> vertexSet;
	private HashMap<Integer, Integer> degreeList;
	private HashMap<Integer, ArrayList<Integer>> edgeList;
	private int edgeSize;
	private int vertexSize;
	
	public Graph(String graphFilePath){
		vertexSet = new HashSet<Integer>();
		degreeList = new HashMap<Integer,Integer>();
		edgeList = new HashMap<Integer, ArrayList<Integer>>();
		edgeSize = 0;
		vertexSize = 0;
		loadGraphFromEdge(graphFilePath);
	}
	
	
	public Graph(String graphFilePath, boolean isEdge){
		vertexSet = new HashSet<Integer>();
		degreeList = new HashMap<Integer,Integer>();
		edgeList = new HashMap<Integer, ArrayList<Integer>>();
		edgeSize = 0;
		vertexSize = 0;
		if(isEdge)
			loadGraphFromEdge(graphFilePath);
		else
			loadGraph(graphFilePath);
	}
	
	public int getDegree(int vid){
		return degreeList.get(vid);
	}
	
	public Set<Integer> getVertexSet(){
		return vertexSet;
	}
	
	public int getVertexSize(){
		return vertexSize;
	}
	
	public int getEdgeSize(){
		return edgeSize;
	}
	
	public ArrayList<Integer> getNeighbors(int vid){
		return edgeList.get(vid);
	}
	
	private void loadGraphFromEdge(String graphFilePath){
		try {
			FileInputStream fin = new FileInputStream(graphFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			String line;
			while((line = fbr.readLine()) != null){
				if(line.startsWith("#")) continue;
				String [] values = SEPERATOR.split(line);
				if(values.length != 2){
					System.out.println("Edge Fromate Required. Error Line: "+line+". parsed value size = "+values.length);
					continue;
				}
				int sv = Integer.valueOf(values[0]);
				int ev = Integer.valueOf(values[1]);
				if(vertexSet.add(sv)){
					vertexSize++;
					degreeList.put(sv, 0);
					edgeList.put(sv, new ArrayList<Integer>());
				}
				if(vertexSet.add(ev)){
					vertexSize++;
					degreeList.put(ev, 0);
					edgeList.put(ev, new ArrayList<Integer>());
				}
				degreeList.put(sv, degreeList.get(sv)+1);
				/* loop */
				if(sv == ev){
					degreeList.put(sv, degreeList.get(sv)+1);
					edgeSize++;
				}
				edgeList.get(sv).add(ev);
				
				edgeSize ++;
			}
			int degreeSum = 0;
			for(int vid: degreeList.keySet()){
				degreeSum += degreeList.get(vid);
			}
			System.out.println("Vertex="+vertexSize+" Edge="+edgeSize +" degreeSum="+degreeSum);
			fbr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * This is load from adjacency format
	 * @param graphFilePath
	 */
	private void loadGraph(String graphFilePath){
		try {
			FileInputStream fin = new FileInputStream(graphFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			String line;
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				int vid = Integer.valueOf(values[0]);
				ArrayList<Integer> al = new ArrayList<Integer>();
				for(int i = 1; i < values.length; ++i){
					al.add(Integer.valueOf(values[i]));
				}
				vertexSet.add(vid);
				degreeList.put(vid, values.length - 1);
				edgeList.put(vid, al);
				vertexSize++;
				edgeSize += values.length - 1;
			}
			System.out.println("Vertex="+vertexSize+" Edge="+edgeSize);
			fbr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
