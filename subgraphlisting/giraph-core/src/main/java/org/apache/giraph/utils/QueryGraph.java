package org.apache.giraph.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;

public class QueryGraph {

	private static final Logger LOG = Logger.getLogger(QueryGraph.class);
	
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	
	/* index is key. */
	private final HashMap<Integer, ArrayList<Integer>> edgelist = new HashMap<Integer, ArrayList<Integer>>();
	/* index is key */
	private final HashMap<Integer, Integer> labellist = new HashMap<Integer, Integer>();
	/* vertex id is key */
	private final HashMap<Integer, Integer> nodeIdToIndex = new HashMap<Integer, Integer>();
	/* record neighbor labels */
	private final HashMap<Integer, BitMap> neighborLabel = new HashMap<Integer, BitMap>();
	
	private int totalEdges = 0;
	
	public QueryGraph(){
		
	}
	
	/**
	 * Assume the query graph format:
	 * vertex id, label, neighbor lists
	 * @param conf
	 */
	public void load(Configuration conf){
		try {
			FileSystem fs = FileSystem.get(conf);
			Path queryGraphPath = new Path(conf.get(GiraphConstants.GIRAPH_SUBGRAPHMATCH_QUERYGRAPHPATH, 
					GiraphConstants.DEFAULT_GIRAPH_SUBGRAPHMATCH_QUERYGRAPHPATH));			
			FSDataInputStream queryGraphStream = fs.open(queryGraphPath);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(queryGraphStream, Charsets.UTF_8));			
		  
			String line;
			int index = 0;
			int queryVertexId;
			while ((line = reader.readLine()) != null) {				
			  if (line.length() == 0)					
				  continue;
			  String[] values = SEPARATOR.split(line);
			  queryVertexId = Integer.valueOf(values[0]);
			  
			  nodeIdToIndex.put(queryVertexId, index);
			  labellist.put(index, Integer.valueOf(values[1]));
			  
			  ArrayList<Integer> neighborList = new ArrayList<Integer>();
			  for(int i = 2; i < values.length; i++){
				  neighborList.add(Integer.valueOf(values[i]));
				  totalEdges++;
			  }
			  edgelist.put(index, neighborList);
			  ++index;
			}
			
			
			/* extract bit map */
			for(int idx : edgelist.keySet()){
				ArrayList<Integer> al = edgelist.get(idx);
				BitMap bm = new BitMap();
				for(int neighbor : al){
					bm.set(labellist.get(nodeIdToIndex.get(neighbor)));
				}
				neighborLabel.put(idx, bm);
			}
			queryGraphStream.close();
			LOG.info("Total Edge="+totalEdges);
		} catch (Exception e) {
			System.out.println("QueryGraph loading Exception.");
			e.printStackTrace();
		}	
	}
	
	public int getNodeIndex(final int nodeId){
		return nodeIdToIndex.get(nodeId);
	}
	
	public Set<Integer> getNodeSet(){
		return nodeIdToIndex.keySet();
	}
	
	/**
	 * TODO: assume the graph is undirect. 
	 * @return
	 */
	public int getTotalEdges(){
		return (totalEdges >> 1);
	}
	
	/**
	 * get Edge list for a node
	 * @param index
	 * @return
	 */
	public ArrayList<Integer> getEdgeList(int index){
		return edgelist.get(index);
	}
	
	/**
	 * get vertex's neighbor labels
	 * @param index
	 * @return
	 */
	public BitMap getNeighborLabel(int index){
		return neighborLabel.get(index);
	}

	/**
	 * get Label of a node
	 * @param queryVertexId
	 * @return
	 */
	public int getLabel(int queryVertexId) {
		return labellist.get(nodeIdToIndex.get(queryVertexId));
	}
	
	/**
	 * get degree of a node
	 * @param queryVertexId
	 * @return
	 */
	public int getDegree(int queryVertexId){
		return getEdgeList(nodeIdToIndex.get(queryVertexId)).size();
	}

	/**
	 * get the size of query graph
	 * @return
	 */
	public int getNodeSize() {
		return nodeIdToIndex.size();
	}	

//	public ArrayList<Integer> getNodeIndexList(int label) {
//		ArrayList<Integer> results = new ArrayList<Integer>();
//		for(int key : labellist.keySet()){
//			if(labellist.get(key) == label){
//				results.add(key);
//			}
//		}
//		return results.size() == 0 ? null : results;
//	}

	/**
	 * get the first node which has "label" in keySet   
	 * @param label
	 * @return
	 */
	public int getNodeIndexByLabel(int label) {
		for(int key : labellist.keySet()){
			if(labellist.get(key) == label){
				return key;
			}
		}
		return -2;
	}
	
	/**
	 * 
	 */
	public String toString(){
		String ans ="";
		ans = "nodeToIndex: ";
		for(Integer node : nodeIdToIndex.keySet()){
			ans +="("+node+", "+nodeIdToIndex.get(node)+") ";
		}
		ans = ans + "\nLabelList: ";
		for(Integer index : labellist.keySet()){
			ans +="("+index+", "+labellist.get(index)+") ";
		}
		ans = ans + "\nEdgelist:\n";
		for(Integer index : edgelist.keySet()){
			ArrayList<Integer> edges = edgelist.get(index);
			ans = ans + index + " ";
			for(int i = 0; i < edges.size(); i++){
				ans += " "+ edges.get(i);
			}
			ans += "\n";
		}
		ans = ans +"NeighborLabel:\n";
		for(Integer index : neighborLabel.keySet()){
			ans = ans + index+" " + neighborLabel.get(index).toString()+"\n";
		}
		
		return ans;
	}
}
