package com.graphtools.subgraphmatch;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import com.graphtools.utils.AutomorphismDetector;
import com.graphtools.utils.DisjointSets;
import com.graphtools.utils.EdgeOrientation;
import com.graphtools.utils.VertexGroup;

public class TopoQueryEnumerator 
extends AutomorphismDetector{
	
	/* input */
//	private String queryGraphPath;
//	private String savePath;

	/* output: record the independent sequenced query */
	private ArrayList<Integer> nodeSequence;
	private ArrayList<Edge> edgeSequence;
	private ArrayList<Long> edgeAssignmentList; //Assume the edge number does not exceed 64.
	private ArrayList<Long> edgeAssignmentListBackup;
	
	private ArrayList<EdgeOrientation> validEdgeOrientationList;
	
	private int[] index;
	private int inDegree1[];
	private int inDegree2[];
	private int outDegree1[];
	private int outDegree2[];
	
	/* automorphism related structure */
	
	public TopoQueryEnumerator(){
//		this.queryGraphPath = null;
//		this.savePath = null;
		super();
		this.nodeSequence = null;
		this.edgeSequence = null;
		this.edgeAssignmentList = null;
		validEdgeOrientationList = new ArrayList<EdgeOrientation>();
	}
	
	
//	public QueryEnumerator(String queryGraphPath, String savePath){
//		this.queryGraphPath = queryGraphPath;
//		this.savePath = savePath;
//		this.nodeSequence = null;
//		this.edgeSequence = null;
//		this.edgeAssignmentList = null;
//	}
	
//	public void setQueryGraphPath(String queryGraphPath){
//		this.queryGraphPath = queryGraphPath;
//	}
//	
//	public void setSavePath(String savePath){
//		this.savePath = savePath;
//	}

	public void doEnumeration(){
		this.initialize();
		this.enumeration();
	}
	
	private void initialize(){
		
		if(isLoadGraph == false){
			System.out.println("Please Load graph before do the enumeration.");
			return ;
		}
		
		/**
		 * 1. generate all automorphism or the query graph
		 * 2. generate node sequence (Random)
		 * 3. generate edge sequence (BFS/DFS)
		 */
		nodeSequence = new ArrayList<Integer>();
		this.edgeAssignmentList = new ArrayList<Long>();
		this.edgeAssignmentListBackup = new ArrayList<Long>();
		
		/* copy the node sequence */
		for(int i = 0; i < vertexList.size(); i++){
			nodeSequence.add(vertexList.get(i));
		}
		
		/* BFS to get an edge sequence */
		edgeSequence = getEdgeSequence();
		System.out.println("Edge Sequence Size = " + edgeSequence.size());
		for(int i = 0; i < edgeSequence.size(); i++){
			System.out.println("Edge "+edgeSequence.get(i).getId()
					+":"+"<"+edgeSequence.get(i).getSource()
					+","+edgeSequence.get(i).getTarget()+">");
		}
		
//		System.out.println(isValid(0));
//		System.out.println(isValid(1));
//		
//		System.exit(0);
		/* do the initial edge assignment */
		int edgeNumber = edgeSequence.size();
		for(long instance = 0; instance < (1L<<edgeNumber); instance++){
//			if(isValid(instance)){
				edgeAssignmentList.add(instance);
//			}
		}
	}
	
	/**
	 * NP-complete problem
	 * @param assignment
	 * @return
	 */
	private boolean isValid(long assignment){
//		boolean isOk = true;
		EdgeOrientation eo = getEdgeOrientation(assignment);
		return eo.checkValid();
	}
	
	/**
	 * using BFS to generate an sequence
	 * @return
	 */
	private ArrayList<Edge> getEdgeSequence(){
		ArrayList<Edge> edgeSequence = new ArrayList<Edge>();
		
		HashMap<Integer, Integer> isVisited = new HashMap<Integer, Integer>();
		LinkedList<Integer> queue = new LinkedList<Integer>();
		int eid = 0;
		queue.push(vertexList.get(0));
		/* in queue */
		isVisited.put(vertexList.get(0), 1);
		while(queue.isEmpty() == false){
			int curVid = queue.getFirst();
			queue.remove();
			
			/* out queue */
			isVisited.put(curVid, 2);
			
			ArrayList<Integer> neighborList = graph.get(curVid);
			for(int i = 0; i < neighborList.size(); i++){
				int nid = neighborList.get(i);
				
				if(isVisited.get(nid) == null || isVisited.get(nid) == 1){
					edgeSequence.add(new Edge(eid, curVid, nid));
					eid++;
				}
				
				if(isVisited.get(nid) == null){
					isVisited.put(nid, 1);
					queue.push(nid);
				}
			}
		}
		return edgeSequence;
	}
	
	private void enumeration(){
		/**
		 * enumerating automorphism, and shrink edge assignment
		 * TODO: implement DFS version first.
		 */
		simpleAutomorphismEnumerator();
		for(HashMap<Integer, Integer> automorphismMap : this.getAutomorphismList()){
			eliminateAutomorphismAssignment(automorphismMap);
		}
		
		/**
		 * fiter invalid assignment here
		 */
		edgeAssignmentListBackup.clear();
		for(Long assignment : this.edgeAssignmentList){
			if(isValid(assignment)){
//				System.out.println("Assignment: "+assignment+" ==> valid");
				edgeAssignmentListBackup.add(assignment);
			}
			else{
//				System.out.println("Assignment: "+assignment+" ==> inValid");
			}
		}
		/* swap the edge assignment list */
		ArrayList<Long> tmp = this.edgeAssignmentListBackup;
		this.edgeAssignmentListBackup = this.edgeAssignmentList;
		this.edgeAssignmentList = tmp;
		
		/* DEBUG INFO */
//		System.out.println("Shrinked Edge Assginment:");
//		for(int i = 0; i < edgeAssignmentList.size(); i++){
//			String line = i +": " + edgeAssignmentList.get(i) + "\n";
//			System.out.print(line);
//		}
//		System.out.println("After shrink edge assignment!");
		
		/**
		 * prune automorphism with edge label
		 * dsdafjklajfkl;ajf
		 */
		for(long edgeAssignment : edgeAssignmentList){
			EdgeOrientation eo = getEdgeOrientation(edgeAssignment);
			System.out.println("Edge Orientation: ");
			System.out.println(eo.toString());
			expandQuery(eo, edgeAssignment);
		}
	}
	
	private void expandQuery(EdgeOrientation eo, long edgeAssignment) {
		int size = vertexList.size();
		int inDegree[] = new int[size];
		int[] seq = new int[size];
		for (int i = 0; i < size; ++i) {
			for (int j = 0; j < size; ++j)
				if (eo.getEdgeOrientation(i, j) == 1) inDegree[i]++;
		}
		boolean vis[] = new boolean[size];
		for (int i = 0; i < size; ++i) {
			vis[i] = false;
		}
		ArrayList<EdgeOrientation> assignedEdgeOrientationList = new ArrayList<EdgeOrientation>();
		dfsExpand(inDegree, size, seq, 0, vis, eo, assignedEdgeOrientationList, edgeAssignment);
	}


	private void dfsExpand(int[] inDegree, int size, int[] seq, int tot, boolean[] vis,
			EdgeOrientation eo, ArrayList<EdgeOrientation> el, long assign) {
		if (tot == size) {
			EdgeOrientation permutation = getEdgeOrientation(assign);
			
			for (int i = 0; i < size - 1; ++i) {
				permutation.setEdgeOrientation(seq[i], seq[i + 1], EdgeOrientation.EDGE_ORIENTATION_BIG);
				permutation.setEdgeOrientation(seq[i + 1], seq[i], EdgeOrientation.EDGE_ORIENTATION_SMALL);
			}
			
			for (EdgeOrientation tmp : el) {
				if (checkIsomorphism(tmp, permutation)) return;
			}
			System.out.println("size: " + size);
			System.out.println("permutation: ");
			//System.out.println(permutation.toString());
			for (int i = 0; i < size; ++i)
				System.out.print(seq[i] + " ");
			System.out.println();
			
			el.add(permutation);
			validEdgeOrientationList.add(permutation);
			return;
		}
		
		for (int i = 0; i < size; ++i) {
			if (inDegree[i] == 0 && !vis[i]) {
				seq[tot++] = i;
				for (int j = 0; j < size; ++j)
					if (eo.getEdgeOrientation(i, j) == 2) inDegree[j]--;
				vis[i] = true;
				dfsExpand(inDegree, size, seq, tot, vis, eo, el, assign);
				tot--;
				for (int j = 0; j < size; ++j)
					if (eo.getEdgeOrientation(i, j) == 2) inDegree[j]++;
				vis[i] = false;
			}
		}
	}
	
	private boolean checkIsomorphism(EdgeOrientation e1, EdgeOrientation e2) {
		int size = vertexList.size();
		index = new int[size];
		inDegree1 = new int[size];
		inDegree2 = new int[size];
		outDegree1 = new int[size];
		outDegree2 = new int[size];
		for (int i = 0; i < size; ++i) {
			index[i] = -1;
			for (int j = 0; j < size; ++j) {
				if (e1.getEdgeOrientation(i, j) == 1) inDegree1[i]++;
				if (e1.getEdgeOrientation(i, j) == 2) outDegree1[i]++;
				if (e2.getEdgeOrientation(i, j) == 1) inDegree2[i]++;
				if (e2.getEdgeOrientation(i, j) == 2) outDegree2[i]++;
			}
		}
		if (dfsCheck(0, size, e1, e2)) return true;
			else return false;
	}
	
	private boolean dfsCheck(int tot, int size, EdgeOrientation e1, EdgeOrientation e2) {
		if (tot == size)
			return true;
	L1: for (int i = 0; i < size; ++i) {
			if (index[i] >= 0) continue;
			if (inDegree1[tot] != inDegree2[i] || outDegree1[tot] != outDegree2[i]) continue;
			for (int j = 0; j < size; ++j) {
				if (index[j] < 0) continue;
				if (e2.getEdgeOrientation(i, j) != e1.getEdgeOrientation(tot, index[j])) continue L1;
			}
			index[i] = tot;
			//System.out.println("Map : " + tot + " " + i);
			if (dfsCheck(tot + 1, size, e1, e2)) return true;
			index[i] = -1;
		}
		return false;
	}

	private void addVertexSequenceConstrain(
			ArrayList<VertexGroup> vertexGroupList, 
			ArrayList<HashMap<Integer, Integer>> automorphismList) {
		for(VertexGroup vg : vertexGroupList){
			vg.generateSequence(automorphismList);
		}
	}

	private ArrayList<VertexGroup> constructGroups(ArrayList<HashMap<Integer, Integer>> automorphismList){
		ArrayList<VertexGroup> vertexGroupList = new ArrayList<VertexGroup>();
		DisjointSets ds = new DisjointSets(vertexList.size());
		for(HashMap<Integer, Integer> automorphismMap : automorphismList){
			for(int vid : automorphismMap.keySet()){
				/* need DisjointSets structure here! */
				if(vid != automorphismMap.get(vid)){
//					System.out.println("UNION: vid="+vid+", mappedVid="+automorphismMap.get(vid));
					int vidIndex = vertexIdIndexMap.get(vid);
					int mapVidIndex = vertexIdIndexMap.get(automorphismMap.get(vid));
					if(ds.find(vidIndex) != ds.find(mapVidIndex)){
						ds.union(vidIndex, mapVidIndex);
					}
				}
			}
		}
		
		for(int vid : vertexList){
			int index = vertexIdIndexMap.get(vid);
			boolean exist = false;
			int gid = ds.find(index);
//			System.out.println("vid ="+vid+", gid="+gid);
			for(VertexGroup vg : vertexGroupList){
				if(vg.isSameGroup(gid)){
					exist = true;
					vg.add(vid);
					break;
				}
			}
			if(!exist){
				VertexGroup vg = new VertexGroup(gid);
				vg.add(vid);
				vertexGroupList.add(vg);
			}
		}
		return vertexGroupList;
	}
	
	private EdgeOrientation getEdgeOrientation(long edgeAssignment) {
		EdgeOrientation eo = new EdgeOrientation(vertexList.size());
		int index = 0;
		for(int vid : vertexList){
			eo.setVertexIdIndex(vid, index);
			index++;
		}
		for(Edge edge : edgeSequence){
			int id = edge.id;
			byte orientation = (byte)(edgeAssignment >> id & 1L);
			eo.setEdgeOrientation(eo.getVertexIndex(edge.getSource()), eo.getVertexIndex(edge.getTarget()), (byte) (orientation + 1));
			orientation ^=1; /* reverse the edge */
			eo.setEdgeOrientation(eo.getVertexIndex(edge.getTarget()), eo.getVertexIndex(edge.getSource()), (byte) (orientation + 1));
		}
		return eo;
	}


	public void saveIndependentQuery(String fileSavePath){
		try {
			String queryGraphPath = fileSavePath;
			String edgeOrientationPath = fileSavePath + ".edgeorientation";
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(queryGraphPath));
			DataOutputStream out = new DataOutputStream(new FileOutputStream(edgeOrientationPath));
			
			int sameLabel = 1;
			for(int vid : graph.keySet()){
				String line = vid +" " + sameLabel;
				ArrayList<Integer> neighbor = graph.get(vid);
				for(int i = 0; i < neighbor.size(); i++){
					line += " "+neighbor.get(i);
				}
				bw.write(line);
				bw.newLine();
			}
			
			out.writeInt(validEdgeOrientationList.size());
			for(int i = 0; i < validEdgeOrientationList.size(); i++){
				validEdgeOrientationList.get(i).write(out);
			}
			
			out.close();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void eliminateAutomorphismAssignment(
			HashMap<Integer, Integer> vertexMapList) {
		HashSet<Long> cleanEdgeAssignmentSet = new HashSet<Long>();
		this.edgeAssignmentListBackup.clear();
		for(Long assignment : this.edgeAssignmentList){
			if(cleanEdgeAssignmentSet.contains(assignment)){
				continue;
			}
			
			edgeAssignmentListBackup.add(assignment);
			long dupAssignment = 0;
			for(Edge edge : edgeSequence){
				int id = edge.getId();
//				System.out.println("<"+edge.getSource()+", "+edge.getTarget()+"> ==> <"
//				+vertexMapList.get(edge.getSource())+","+vertexMapList.get(edge.getTarget())+">");
				long orientation = getOrientation(new Edge(id, vertexMapList.get(edge.getSource()), vertexMapList.get(edge.getTarget())), assignment);
				dupAssignment +=  orientation << id;
			}
			if(dupAssignment != assignment){
				cleanEdgeAssignmentSet.add(dupAssignment);
			}
		}
		/* swap the edge assignment list */
		ArrayList<Long> tmp = this.edgeAssignmentListBackup;
		this.edgeAssignmentListBackup = this.edgeAssignmentList;
		this.edgeAssignmentList = tmp;
	}

	private long getOrientation(Edge edge, Long assignment) {
		for(Edge tmpEdge : edgeSequence){
			int id = tmpEdge.getId();
//			System.out.println("tmpEdge: <"+tmpEdge.getSource()+", "+tmpEdge.getTarget()+">");
			if((tmpEdge.getSource() == edge.getSource() && tmpEdge.getTarget() == edge.getTarget())){
				return ((assignment >> id) & 1L);
			}
			if((tmpEdge.getSource() == edge.getTarget() && tmpEdge.getTarget() == edge.getSource())){
				return (((assignment >> id) & 1L)^1L);
			}
		}
		System.out.println("Error: Edge Not Found!");
		System.exit(-1);
		return 0;
	}
	
	private class Edge{
		private int id;
		private int source;
		private int target;
		
		public Edge() {}
		public Edge(int id, int source, int target){
			this.id = id;
			this.source = source;
			this.target = target;
		}
		
		public void setId(int id){
			this.id = id;
		}
		
		public int getId(){
			return this.id;
		}
		
		public void setSource(int source){
			this.source = source;
		}
		
		public int getSource(){
			return this.source;
		}
		
		public void setTarget(int target){
			this.target = target;
		}
		
		public int getTarget(){
			return this.target;
		}
	}
}
