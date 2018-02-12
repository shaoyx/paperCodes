package com.graphtools.subgraphmatch;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

public class RandomQueryGenerator 
extends GraphLoader {
	
	private HashMap<Integer, ArrayList<Integer>> queryGraph;
	
	private String type;
	
	private int stepLimit;
	private int edgeLimit;
	private int vertexLimit;
	
	public RandomQueryGenerator(int stepLimit, int edgeLimit, int vertexLimit){
		this.stepLimit = stepLimit;
		this.edgeLimit = edgeLimit;
		this.vertexLimit = vertexLimit;
		type = "dfs";
		vertexLabelMap = null;
		graph = null;
		queryGraph = null;
		vertexList = null;
	}
	
	public RandomQueryGenerator(){
		this(20,20,20);
	}

	public void generateRandomQuery(){
		if(vertexLabelMap == null || graph == null){
			System.out.println("Please load graph and label file before generating random query!");
			return;
		}
		if("dfs".equals(type.trim().toLowerCase())){
			System.out.println("dfs generator");
			dfsGenerator();
		}
		else if("bfs".equals(type.trim().toLowerCase())){
			System.out.println("bfs generator");
			bfsGenerator();
		}
		else if("strictdfs".equals(type.trim().toLowerCase())){
			System.out.println("strictdfs generator");
			strictDfsGenerator();
		}
		else{
			System.out.println("unsupported generator type: "+ type +"\nValid type is bfs or dfs");
		}
	}
	
	private void dfsGenerator(){
		queryGraph = new HashMap<Integer, ArrayList<Integer>>();
		Random rand = new Random();
		int step = 0;
		int edge = 0;
		int vertex = 0;
		
		int curVertex = vertexList.get(Math.abs(rand.nextInt()) % vertexList.size());
		queryGraph.put(curVertex, new ArrayList<Integer>());
		vertex++;
		while(step < stepLimit && edge < edgeLimit && vertex < vertexLimit){
			ArrayList<Integer> neighbors = graph.get(curVertex);
			int nextVertex = neighbors.get(Math.abs(rand.nextInt()) % neighbors.size());
			ArrayList<Integer> al;
			ArrayList<Integer> al2;
			if(queryGraph.get(nextVertex) == null){
				al = new ArrayList<Integer>();
				al.add(curVertex);
				queryGraph.put(nextVertex, al);
				
				al2 = queryGraph.get(curVertex);
				al2.add(nextVertex);
				queryGraph.put(curVertex, al2);
				vertex++;
				edge++;
			}
			else{
				al = queryGraph.get(nextVertex);
				boolean visited = false;
				for(int value : al){
					if(value == curVertex){
						visited = true;
						break;
					}
				}
				if(!visited){
					al.add(curVertex);
					queryGraph.put(nextVertex, al);
					
					al2 = queryGraph.get(curVertex);
					al2.add(nextVertex);
					queryGraph.put(curVertex, al2);
					vertex++;
					edge++;
				}
			}
			step++;
			curVertex = nextVertex;
		}
	}
	
	private void strictDfsGenerator(){
		queryGraph = new HashMap<Integer, ArrayList<Integer>>();
		Random rand = new Random();
		int step = 0;
		int edge = 0;
		int vertex = 0;
		
		int curVertex = vertexList.get(Math.abs(rand.nextInt()) % vertexList.size());
		queryGraph.put(curVertex, new ArrayList<Integer>());
		vertex++;
		while(step < stepLimit && edge < edgeLimit && vertex < vertexLimit){
			ArrayList<Integer> neighbors = graph.get(curVertex);
			int nextVertex = neighbors.get(Math.abs(rand.nextInt()) % neighbors.size());
			ArrayList<Integer> al;
			ArrayList<Integer> al2;
			if(queryGraph.get(nextVertex) == null){
				al = new ArrayList<Integer>();
				al.add(curVertex);
				queryGraph.put(nextVertex, al);
				
				al2 = queryGraph.get(curVertex);
				al2.add(nextVertex);
				queryGraph.put(curVertex, al2);
				vertex++;
				edge++;
			}
			else{
				al = queryGraph.get(nextVertex);
				boolean visited = false;
				for(int value : al){
					if(value == curVertex){
						visited = true;
						break;
					}
				}
				if(!visited){
					al.add(curVertex);
					queryGraph.put(nextVertex, al);
					
					al2 = queryGraph.get(curVertex);
					al2.add(nextVertex);
					queryGraph.put(curVertex, al2);
					vertex++;
					edge++;
				}
			}
			step++;
			curVertex = nextVertex;
		}
		
		System.out.println("edge="+edge+" edgelimit="+edgeLimit);
		if(edge < edgeLimit){
			Integer[] selectedVertex = new Integer[queryGraph.keySet().size()];
			queryGraph.keySet().toArray(selectedVertex);
			
			ArrayList<Integer> start = new ArrayList<Integer>();
			ArrayList<Integer> end = new ArrayList<Integer>();
			
			for(int selv : selectedVertex){
				ArrayList<Integer> neighbors = graph.get(selv);
				for(int neighbor : neighbors){
					if(queryGraph.get(neighbor) != null){
						if(queryGraph.get(neighbor).contains(selv) == false
								&& start.contains(neighbor) == false){
							start.add(selv);
							end.add(neighbor);
						}
					}
				}
			}
			
			System.out.println("Candidate size="+start.size());
			/*Use Collections.shuffle() twice, with two Random objects initialized with the same seed*/
			long seed = System.nanoTime();
			Collections.shuffle(start, new Random(seed));
			Collections.shuffle(end, new Random(seed));
			int idx = 0;
			while(edge < edgeLimit && idx < start.size()){
				int v1 = start.get(idx);
				int v2 = end.get(idx);
				ArrayList<Integer> al = queryGraph.get(v1);
				al.add(v2);
				queryGraph.put(v1, al);
			
				al = queryGraph.get(v2);
				al.add(v1);
				queryGraph.put(v2, al);
				idx++;
				edge++;
			}
		}
	}
	
	private void bfsGenerator(){
		queryGraph = new HashMap<Integer, ArrayList<Integer>>();
		Random rand = new Random();
		int step = 0;
		int edge = 0;
		int vertex = 0;
		
		LinkedList<Integer> queue = new LinkedList<Integer>();
		
		int curVertex = vertexList.get(Math.abs(rand.nextInt()) % vertexList.size());
		queue.add(curVertex);
		queryGraph.put(curVertex, new ArrayList<Integer>());
		vertex++;
		while(step < stepLimit && edge < edgeLimit && vertex < vertexLimit && queue.isEmpty() == false){
			
			curVertex = queue.poll();
			
			ArrayList<Integer> neighbor = graph.get(curVertex);
			
			queryGraph.put(curVertex, neighbor);
			
			for(int value : neighbor){
				ArrayList<Integer> al = queryGraph.get(value);
				if(al == null){
					al = new ArrayList<Integer>();
					vertex++;
					queue.add(value);
				}
				
				if(!al.contains(curVertex)){
					al.add(curVertex);
					queryGraph.put(value, al);
					edge++;
				}
			}			
		}
	}
	
	public void saveRandomQuery(String path){
		try{
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
			HashMap<Integer, Integer> vertexIdMap = new HashMap<Integer, Integer>();
			int curIdx = 1;
			for(int vid : queryGraph.keySet()){
				Integer vertexId = vertexIdMap.get(vid);
				if(vertexId == null){
					vertexId = curIdx++;
					vertexIdMap.put(vid, vertexId);
				}
				String line =  vertexId + " " + vertexLabelMap.get(vid);
				ArrayList<Integer> al = queryGraph.get(vid);
				
				for(int value : al){
					vertexId = vertexIdMap.get(value);
					if(vertexId == null){
						vertexId = curIdx++;
						vertexIdMap.put(value, vertexId);
					}
					line += " "+ vertexId;
				}
				bw.write(line);
				bw.newLine();
			}
			bw.flush();
			bw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	
	public void setGeneratorType(String type){
		this.type = type;
	}
	
	public void setStepLimit(int stepLimit){
		this.stepLimit = stepLimit;
	}
	
	public void setEdgeLimit(int edgeLimit){
		this.edgeLimit = edgeLimit;
	}
	
	public void setVertexLimit(int vertexLimit){
		this.vertexLimit = vertexLimit;
	}
}
