package com.graphtools.subgraphmatch;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import com.graphtools.utils.EdgeOrientation;
import com.graphtools.utils.VertexGroup;

public class QuerySymmetryBroker 
extends QueryEnumerator{
	
	private EdgeOrientation validEO = null;
	
	public QuerySymmetryBroker(){
		super();
	}
	
	public void doEnumeration(){
		/**
		 * 1. Initialize EdgeOrientation here.
		 */
		validEO = new EdgeOrientation(vertexList.size());
		int index = 0;
		for(int vid : vertexList){
			validEO.setVertexIdIndex(vid, index);
			index++;
		}

//		System.out.println("Begin the broker!!!!!");
//		System.out.println(validEO.toString());
		do{
			/*set the latest edge orientation*/
			this.setEdgeOrientation(validEO);
			/*find the automorphism group*/
			this.simpleAutomorphismEnumerator();
			if(this.getAutomorphismList().size() == 0){
				break;
			}
			/*update edge orientation*/
			ArrayList<VertexGroup> vertexGroupList = 
					constructGroups(this.getAutomorphismList());
			VertexGroup selectedVG = null;
			for(VertexGroup vg : vertexGroupList){
				if(selectedVG == null || vg.size() > selectedVG.size() ){//||
					//select the big id pair rule
					//	(vg.size() == selectedVG.size() && vg.getGroup().get(0) > selectedVG.getGroup().get(0))){
					selectedVG = vg;
				}
			}
			if(selectedVG.size() == 1) 
				break;
//			System.out.println(selectedVG.toString());
			ArrayList<Integer> groupEle = selectedVG.getGroup();
//			System.out.println(groupEle.get(0));
			for(int i = 1; i < groupEle.size(); i++){
				validEO.setEdgeOrientation(validEO.getVertexIndex(groupEle.get(0)), 
						validEO.getVertexIndex(groupEle.get(i)), EdgeOrientation.EDGE_ORIENTATION_SMALL);
				validEO.setEdgeOrientation(validEO.getVertexIndex(groupEle.get(i)), 
						validEO.getVertexIndex(groupEle.get(0)), EdgeOrientation.EDGE_ORIENTATION_BIG);
//				System.out.println(groupEle.get(i));
			}
//			System.out.println(validEO.toString());
//			System.out.println("End one Iteration!");
		}while(true);
//		System.out.println("End the broker!!!!!");
	}
	
	public void saveIndependentQuery(String fileSavePath){
		try {
			
			if(this.enableMinimal){
				seperatedSave(fileSavePath);
				return ;
			}
			
			String queryGraphPath = fileSavePath;
			String edgeOrientationPath = fileSavePath + ".edgeorientation";
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(queryGraphPath));
			
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
			
			//for debug

			System.out.println(validEO.toString());
			System.out.println("Before manually process");
		for(int i = 0; i< 2; i++){
			DataOutputStream out = new DataOutputStream(new FileOutputStream(edgeOrientationPath+"_manually_"+i));
			if(i == 0){
				validEO.setEdgeOrientation(validEO.getVertexIndex(3), 
						validEO.getVertexIndex(4), EdgeOrientation.EDGE_ORIENTATION_BIG);
				validEO.setEdgeOrientation(validEO.getVertexIndex(4), 
						validEO.getVertexIndex(3), EdgeOrientation.EDGE_ORIENTATION_SMALL);
			}
			else{
				validEO.setEdgeOrientation(validEO.getVertexIndex(3), 
						validEO.getVertexIndex(4), EdgeOrientation.EDGE_ORIENTATION_SMALL);
				validEO.setEdgeOrientation(validEO.getVertexIndex(4), 
						validEO.getVertexIndex(3), EdgeOrientation.EDGE_ORIENTATION_BIG);
			}
			out.writeInt(1);
			validEO.calClosure();
			System.out.println("Closure: \n"+validEO.toString());
			System.out.println();
			validEO.write(out);
			
			out.close();

			validEO.setEdgeOrientation(validEO.getVertexIndex(3), 
					validEO.getVertexIndex(4), EdgeOrientation.EDGE_ORIENTATION_UNDEFINED);
			validEO.setEdgeOrientation(validEO.getVertexIndex(4), 
					validEO.getVertexIndex(3), EdgeOrientation.EDGE_ORIENTATION_UNDEFINED);
		}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void seperatedSave(String fileSavePath) {
		ArrayList<Integer> minVertexList = new ArrayList<Integer>(); //store the index
		for(int i = 0; i < vertexList.size(); i++){
			int j;
			for(j = 0; j < vertexList.size(); j++){
				if(this.validEO.getEdgeOrientation(i, j) == EdgeOrientation.EDGE_ORIENTATION_BIG){
					break;
				}
			}
			if(j == vertexList.size()){
				minVertexList.add(i);
			}
		}
		
		int currentId = 0;
		for(int minIdx : minVertexList){
			EdgeOrientation eo = new EdgeOrientation(vertexList.size()); 
			eo.copy(validEO);
			for(int i = 0; i < vertexList.size(); i++){
				if(i == minIdx) continue;
				eo.setEdgeOrientation(minIdx, i, EdgeOrientation.EDGE_ORIENTATION_SMALL);
				eo.setEdgeOrientation(i, minIdx, EdgeOrientation.EDGE_ORIENTATION_BIG);
			}
			

			currentId=minIdx+1;
			try{
				String edgeOrientationPath = fileSavePath + ".edgeorientation."+ currentId;
				DataOutputStream out = new DataOutputStream(new FileOutputStream(edgeOrientationPath));
						
				out.writeInt(1);
				System.out.println(eo.toString());
				System.out.println();
				eo.calClosure();
				System.out.println("Closure: \n"+eo.toString());
				System.out.println();
				eo.write(out);
			
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try{	
			String queryGraphPath = fileSavePath;
		
			BufferedWriter bw = new BufferedWriter(new FileWriter(queryGraphPath));
		
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
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
