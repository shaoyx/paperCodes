package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class HashMapWritable implements Writable{
	private HashMap<Integer, Double> data;
	
	public HashMapWritable() {
//		data = new Hash
	}
	
	public int size(){
		if(data == null)
			return 0;
		return data.size();
	}
	
	public void add(int key, double value){
		if(data == null){
			data = new HashMap<Integer, Double>();
		}
		Double cur = data.get(key);
		if(cur == null){
			cur = 0.0;
		}
		data.put(key, value+cur);
	}
	
	public void divide(double num){
		if(data == null)
			return ;
		for(int vid : data.keySet()){
			data.put(vid, data.get(vid)/num);
		}
	}
	
	public void replace(int vid, double nvalue){
		data.put(vid, nvalue);
	}
	
	public void combine(HashMapWritable messageToCombine) {
		HashMap<Integer, Double> toCombine = messageToCombine.getData();
		if(toCombine == null) 
			return;
		if(data == null){
			data = messageToCombine.getData();
		}
		else{
			for(int vid : toCombine.keySet()){
				if(data.get(vid) == null){
					data.put(vid, toCombine.get(vid));
				}
				else{
					data.put(vid, data.get(vid)+toCombine.get(vid));
				}
			}
		}
	}
	
	public double get(int key){
		if(data == null)
			return 0.0;
		Double res = data.get(key);
		return res == null ? 0.0 : res;
	}
	
	public HashMap<Integer, Double> getData(){
		return data;
	}
	
	public Set<Integer> getKeyEntry(){
		if(data == null)
			return null;
		return data.keySet();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		if(size == 0)
			return ;
		data = new HashMap<Integer, Double>(size);
		int a;
		double b;
		for(int i = 0; i < size; i++){
			a = in.readInt();
			b = in.readDouble();
			data.put(a, b);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if(data == null){
			out.writeInt(0);
			return;
		}
		out.writeInt(data.size());
		for(int vid : data.keySet()){
			out.writeInt(vid);
			out.writeDouble(data.get(vid));
		}
	}

	public String toString(){
		if(data == null)
			return "Empty SimRank!";
		StringBuilder sb = new StringBuilder();
		for(int vid : data.keySet()){
			sb.append("("+vid+","+String.format("%.9f", data.get(vid))+") ");
		}
		return sb.toString();
	}	
}
