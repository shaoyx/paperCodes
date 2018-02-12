package com.graphtools.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

public abstract class AutomorphismDetector {
	protected static final Pattern SEPERATOR =  Pattern.compile("[\t ]");
//	public static final int AUTOMORPHISM_TYPE = 1;
	
	protected HashMap<Integer, ArrayList<Integer>> graph;
	protected ArrayList<Integer> vertexList;
	protected boolean isLoadGraph = false;
	protected HashMap<Integer, Integer> vertexIdIndexMap = null;
	
	private EdgeOrientation edgeOrientation = null;
	private boolean hasEdgeOrientation;
	
	private ArrayList<HashMap<Integer, Integer>> automorphismList = null;
	
	public AutomorphismDetector(){
		hasEdgeOrientation = false;
		automorphismList = new ArrayList<HashMap<Integer, Integer>>();
	}
	
	public void setEdgeOrientation(EdgeOrientation eo){
		this.edgeOrientation = eo;
		this.hasEdgeOrientation = true;
	}
	
	public ArrayList<HashMap<Integer, Integer>> getAutomorphismList(){
		return this.automorphismList;
	}

	/**
	 * Automorphism Detector with no label info
	 */
	public void simpleAutomorphismEnumerator(){
		HashMap<Integer, Integer> degreeDistribution = new HashMap<Integer, Integer>();
		HashMap<Integer, Boolean> vertexUsedMap = new HashMap<Integer, Boolean>();

		/* retrieve degree distribution + initialize vertexUsedMap */
		for(int vertexId : graph.keySet()){
			degreeDistribution.put(vertexId, graph.get(vertexId).size());
			vertexUsedMap.put(vertexId, false);
		}
		
		Integer[] vertexList = new Integer[degreeDistribution.size()];
		HashMap<Integer, Integer> vertexMapList = new HashMap<Integer, Integer>();
		
		degreeDistribution.keySet().toArray(vertexList);
		
		for(int i = 0; i < vertexList.length; i++){
			vertexMapList.put(vertexList[i], -1);
//			System.out.print(vertexList[i]+" ");
		}
//		System.out.println();
//		System.out.println("HasEdgeOrientation="+this.hasEdgeOrientation);
		automorphismList.clear();
		deepthFirstSearchEnumerator(0, degreeDistribution.size(), vertexList, vertexMapList, vertexUsedMap, degreeDistribution);
	}
	
	@SuppressWarnings("unchecked")
	private void deepthFirstSearchEnumerator(int curLevel, int totalLevel, Integer[] vertexList, HashMap<Integer, Integer> vertexMapList, 
			HashMap<Integer, Boolean> vertexUsedMap, HashMap<Integer, Integer> degreeDistribution){
		if(curLevel == totalLevel){
			/* find an automorphism */
//			for(int key : vertexMapList.keySet())
//				System.out.print("<"+key+", "+vertexMapList.get(key)+"> ");
//			System.out.println();
			
//			eliminateAutomorphismAssignment(vertexMapList);
			this.automorphismList.add((HashMap<Integer, Integer>)vertexMapList.clone());
			return ;
		}
		
		int curNode = vertexList[curLevel];
		for(int i = 0; i < vertexList.length; i++){
//			System.out.println("Level="+curLevel);
//			for(int j = 0; j < vertexUsedMap.size(); j++){
//				System.out.print(vertexUsedMap.get(vertexList[j])+" ");
//			}
//			System.out.println();
//			
//			System.out.println("curNode="+curNode +" mappedNode="+vertexList[i]);
//			System.out.println("Conditition1:"+vertexUsedMap.get(vertexList[i]));
//			System.out.println("Conditition2:"+degreeDistribution.get(curNode).equals(degreeDistribution.get(vertexList[i])));
//			System.out.println("Conditition3:"+checkValid(curLevel, i, vertexList, vertexMapList));
			
			if(vertexUsedMap.get(vertexList[i]) == false /* not mapped */
					&& degreeDistribution.get(curNode).equals(degreeDistribution.get(vertexList[i])) /* degree matched */ 
					&& checkValid(curLevel, i, vertexList, vertexMapList) == true /* neighborhood checked */){
//				System.out.println("successed");
				vertexUsedMap.put(vertexList[i], true);
				vertexMapList.put(curNode, vertexList[i]);
				deepthFirstSearchEnumerator(curLevel+1, totalLevel, vertexList, vertexMapList, vertexUsedMap, degreeDistribution);
				vertexMapList.put(curNode, -1);
				vertexUsedMap.put(vertexList[i], false);
			}
		}
	}
	
	private boolean checkValid(int vertexIndex, int mapVertexIndex, Integer[] vertexList, HashMap<Integer, Integer> vertexMapList){
		ArrayList<Integer> neighborList = graph.get(vertexList[vertexIndex]);
		ArrayList<Integer> mapVertexNeighborList = graph.get(vertexList[mapVertexIndex]);
//		System.out.println("vertex="+vertexList[vertexIndex]);
//		System.out.println("mapVertex="+vertexList[mapVertexIndex]);
		for(int nid : neighborList){
			int mapNid = vertexMapList.get(nid);
//			System.out.println("nid="+nid+" mNid="+mapNid);
			if(mapNid != -1){
				boolean exist = false;
				for(int tmpNid : mapVertexNeighborList){
					if(tmpNid == mapNid && (!hasEdgeOrientation || checkOrientation(vertexList[vertexIndex], nid, vertexList[mapVertexIndex], mapNid))){
						exist = true;
						break;
					}
				}
				if(!exist){
					return false;
				}
			}
		}
		
		/*need check the orientation for non neighbor*/
		for(int vid : vertexList){
			int mVid = vertexMapList.get(vid);
			if( mVid != -1 && !checkOrientation(vertexList[vertexIndex], vid, vertexList[mapVertexIndex], mVid)){
				return false;
			}
		}
		return true;
	}
	
	private boolean checkOrientation(Integer vid, int nid,
			Integer mapVid, int mapNid) {
		return edgeOrientation.getEdgeOrientation(edgeOrientation.getVertexIndex(vid), edgeOrientation.getVertexIndex(nid))
				==edgeOrientation.getEdgeOrientation(edgeOrientation.getVertexIndex(mapVid), edgeOrientation.getVertexIndex(mapNid));
	}

	/**
	 * Loading query graph structure with no addition information;
	 * Graph is represented in adjacent format 
	 * @param graphFilePath
	 */
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
//					System.out.println(vid+ " "+Integer.valueOf(values[i]));
					al.add(Integer.valueOf(values[i]));
				}
				vertexList.add(vid);
				graph.put(vid, al);
			}
			
			vertexIdIndexMap = new HashMap<Integer, Integer>();
			for(int i = 0; i < vertexList.size(); i++){
				vertexIdIndexMap.put(vertexList.get(i), i);
			}
			
			isLoadGraph = true;
			fbr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
