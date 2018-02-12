package org.apache.giraph.utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class GeneralQueryGraph 
extends QueryGraph 
implements QueryGraphSequence
{
	private static final Logger LOG = Logger.getLogger(GeneralQueryGraph.class);
	private HashMap<Integer, EdgeOrientation> edgeOrientation = null;
	private ArrayList<Integer> querySequenceTypeList = null;
	
	public GeneralQueryGraph(){
		super();
	}
	
	public void load(Configuration conf){
		
		/* load graph */
		super.load(conf);
		
		/* TODO: load edge Sequence, or manually construction */
		edgeOrientation = new HashMap<Integer, EdgeOrientation>();
		querySequenceTypeList = new ArrayList<Integer>();

		try {
			FileSystem fs = FileSystem.get(conf);
			Path queryGraphEdgeOrientationPath = new Path(conf.get(GiraphConstants.GIRAPH_SUBGRAPHMATCH_QUERYGRAPH_EDGEORIENTATION, 
					GiraphConstants.DEFAULT_GIRAPH_SUBGRAPHMATCH_QUERYGRAPH_EDGEORIENTATION));			
			FSDataInputStream queryGraphStream = fs.open(queryGraphEdgeOrientationPath);	
			
			int edgeOrientationSize = queryGraphStream.readInt();
//			int queryTypeIndex = 1;
			for(int i = 0; i < edgeOrientationSize; i++){
				EdgeOrientation eo = new EdgeOrientation();	
				eo.readFileds(queryGraphStream);
				
				querySequenceTypeList.add(i+1);
				edgeOrientation.put(i+1, eo);
				LOG.info(i+"th: "+eo.toString());
			}
		} catch (Exception e) {
			System.out.println("QueryGraphEdgeOrientation loading Exception.");
			e.printStackTrace();
		}	
		
		/**
		 * Current fixed two query types for square query.
		 */
//		querySequenceTypeList.add(1); //querySequenceTypeList.add(2);
		   /**
		    * Square:
		    *   W --- Z
		    *   |     |
		    *   |     |
		    *   X --- Y
		    *   
		    * Constrains:
		    * E(W,X) & E(X,Y) & E(Y,Z) & E(W,Z)o
		    * W < X < Y < Z
		    * 
		    * E(W,X) & E(Y,X) & E(Y,Z) & E(W,Z)
		    * W < Y < X < Z
		    * 
		    * E(W,X) & E(X,Y) & E(Z,Y) & E(W,Z)
		    * W < X < Z < Y
		    */
	}
	
	public EdgeOrientation getEdgeOrientation(int queryType){
		return edgeOrientation.get(queryType);
	}
	
	public byte getEdgeOrientation(int queryType, int source, int target){
		return edgeOrientation.get(queryType).getEdgeOrientation(source, target);
	}
	
	public void addQuerySequenceType(int querySequenceType){
		querySequenceTypeList.add(querySequenceType);
	}
	
	public ArrayList<Integer> getQuerySequenceTypeList(){
		return this.querySequenceTypeList;
	}
	
	public int getQuerySequenceType(int index){
		return this.querySequenceTypeList.get(index);
	}
}
