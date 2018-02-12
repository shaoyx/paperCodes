package com.graphtools.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class VertexGroup {
	private ArrayList<Integer> group;
	private int groupId;
	private HashSet<ArrayList<Integer>> validSequence;
	private HashSet<ArrayList<Integer>> inValidSequence;
	
	public VertexGroup(int gid){
		group = new ArrayList<Integer>();
		groupId = gid;
		inValidSequence = new HashSet<ArrayList<Integer>>();
		validSequence = new HashSet<ArrayList<Integer>>();
	}
	
	public boolean isSameGroup(int gid){
		return groupId == gid;
	}
	
	public void add(int vertexId){
		group.add(vertexId);
	}
	
	public int size(){
		return group.size();
	}
	
	public ArrayList<Integer> getGroup(){
		return group;
	}
	
	public HashSet<ArrayList<Integer>> getValidSequence(){
		return this.validSequence;
	}

	public void generateSequence(
			ArrayList<HashMap<Integer, Integer>> automorphismList) {
		
		Collections.sort(group);
		Integer [] vertexIdArray = new Integer[group.size()];
		group.toArray(vertexIdArray);
		
//		System.out.println("GroupSize="+group.size());
//		System.out.println(Arrays.toString(vertexIdArray));
		
		inValidSequence.clear();
		do{
			ArrayList<Integer> al = new ArrayList<Integer>();
			for(int i = 0; i < vertexIdArray.length; i++){
				al.add(vertexIdArray[i]);
			}
			/* check valid first */
			if(inValidSequence.contains(al)) continue;
			
			validSequence.add(al);
			
			/* expanding invalid sequence */
			for(HashMap<Integer, Integer> vertexIdMap : automorphismList){
				al = new ArrayList<Integer>();
				for(int i = 0; i < vertexIdArray.length; i++){
					al.add(vertexIdMap.get(vertexIdArray[i]));
				}
				inValidSequence.add(al);
			}
		}while(Permutation.nextPermutation(vertexIdArray) != null);
			
	}
}
