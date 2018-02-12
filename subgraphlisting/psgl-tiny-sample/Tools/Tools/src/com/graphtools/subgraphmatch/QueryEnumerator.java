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

public class QueryEnumerator 
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
	private int saveType = -1;

	protected boolean enableMinimal = false;	
	public void enableMinimal(){
		this.enableMinimal = true;
	}
	
	/* automorphism related structure */
	
	public QueryEnumerator(){
//		this.queryGraphPath = null;
//		this.savePath = null;
		super();
		this.saveType = -1;
		this.nodeSequence = null;
		this.edgeSequence = null;
		this.edgeAssignmentList = null;
		validEdgeOrientationList = new ArrayList<EdgeOrientation>();
	}

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
//		System.out.println("Edge Sequence Size = " + edgeSequence.size());
//		for(int i = 0; i < edgeSequence.size(); i++){
//			System.out.println("Edge "+edgeSequence.get(i).getId()
//					+":"+"<"+edgeSequence.get(i).getSource()
//					+","+edgeSequence.get(i).getTarget()+">");
//		}
		
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
			/**
			 * 1. construct EdgeOrientation Object
			 * 2. do automorphism detecting
			 * 3. construct groups from automorphism
			 * 4. assign sequence for the groups
			 * 5. using automorphism to eliminate equality assignment
			 * 6. save the result 
			 */
			EdgeOrientation eo = getEdgeOrientation(edgeAssignment);
//			System.out.println("EdgeOrientation Before Pruning: "+eo.toString());
			setEdgeOrientation(eo);
			simpleAutomorphismEnumerator();
//			System.out.println("Find automorphism with edgeAssignment="+edgeAssignment);
			
			/* TODO: need to handle no automorphism situation */
			ArrayList<VertexGroup> vertexGroupList = constructGroups(this.getAutomorphismList());
//			System.out.println("The number of vertexGroup = " + vertexGroupList.size());
			addVertexSequenceConstrain(vertexGroupList, getAutomorphismList());
			expandQuery(edgeAssignment, vertexGroupList);
		}
	}
	
	private void expandQuery(long edgeAssignment, ArrayList<VertexGroup> vertexGroupList) {
		ArrayList<ArrayList<ArrayList<Integer>>> vertexSequenceList =
				new ArrayList<ArrayList<ArrayList<Integer>>>();
		for(VertexGroup vg : vertexGroupList){
			ArrayList<ArrayList<Integer>> als = new ArrayList<ArrayList<Integer>>();
			Iterator<ArrayList<Integer>> iter = vg.getValidSequence().iterator();
			while(iter.hasNext()){
				als.add(iter.next());
			}
			vertexSequenceList.add(als);
		}
		int[] groupChosen = new int[vertexGroupList.size()];
		dfsExpand(vertexSequenceList, 0, vertexGroupList.size(), groupChosen, edgeAssignment);
	}


	private void dfsExpand(
			ArrayList<ArrayList<ArrayList<Integer>>> vertexSequenceList, int curLevel,
			int totalLevel, int[] groupChosen, long edgeAssignment) {
		if(curLevel >= totalLevel){
			/* construct EdgeOrientation here */
			EdgeOrientation eo = createEdgeOrientation(vertexSequenceList, groupChosen, edgeAssignment);
//			System.out.println("Edge Orientation: ");
//			System.out.println(eo.toString());
//			System.out.println();
			validEdgeOrientationList.add(eo);
			return;
		}
//		ArrayList<ArrayList<Integer>> validSequence = vertexSequenceList.get(curLevel);
		int size = vertexSequenceList.get(curLevel).size();
		for(int i = 0; i < size; i++){
			groupChosen[curLevel] = i;
			dfsExpand(vertexSequenceList, curLevel+1, totalLevel, groupChosen, edgeAssignment);
		}
	}


	private EdgeOrientation createEdgeOrientation(
			ArrayList<ArrayList<ArrayList<Integer>>> vertexSequenceList,
			int[] groupChosen, long edgeAssignment) {
		EdgeOrientation eo = this.getEdgeOrientation(edgeAssignment);
		for(int i = 0; i < groupChosen.length; i++){
			ArrayList<Integer> autoVertexSequence = vertexSequenceList.get(i).get(groupChosen[i]);
			for(int k = 0; k < autoVertexSequence.size(); k++){
				for(int j = k + 1; j < autoVertexSequence.size(); j++){
					eo.setEdgeOrientation(eo.getVertexIndex(autoVertexSequence.get(k)), 
							eo.getVertexIndex(autoVertexSequence.get(j)), 
							EdgeOrientation.EDGE_ORIENTATION_BIG);
					eo.setEdgeOrientation(eo.getVertexIndex(autoVertexSequence.get(j)), 
							eo.getVertexIndex(autoVertexSequence.get(k)), 
							EdgeOrientation.EDGE_ORIENTATION_SMALL);
				}
			}
		}
		return eo;
	}

	private void addVertexSequenceConstrain(
			ArrayList<VertexGroup> vertexGroupList, 
			ArrayList<HashMap<Integer, Integer>> automorphismList) {
		for(VertexGroup vg : vertexGroupList){
			vg.generateSequence(automorphismList);
		}
	}

	protected ArrayList<VertexGroup> constructGroups(ArrayList<HashMap<Integer, Integer>> automorphismList){
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
			
//			out.writeInt(validEdgeOrientationList.size());
			System.out.println("validEdgeOrientationSize="+validEdgeOrientationList.size());
			if(saveType != -1)
				out.writeInt(1);
			else{
				out.writeInt(validEdgeOrientationList.size());
			}
			for(int i = 0; i < validEdgeOrientationList.size(); i++){
				System.out.println(validEdgeOrientationList.get(i).toString());
				System.out.println();
				if(saveType == -1 || i == saveType){
					validEdgeOrientationList.get(i).calClosure();
					validEdgeOrientationList.get(i).write(out);
				}
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

	public void setSaveType(int saveType) {
		this.saveType = saveType;
	}
}
