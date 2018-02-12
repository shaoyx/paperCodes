package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

public class ArrayWritable implements Writable{
	
	  private List<Integer> neighborList = null;

	  public ArrayWritable() {
	  }

	  public ArrayWritable(List<Integer> neighborList) {
		  this.neighborList = neighborList;
	  }

	  public String[] toStrings() {
	    String[] strings = new String[neighborList.size()];
	    for (int i = 0; i < neighborList.size(); i++) {
	      strings[i] = neighborList.get(i).toString();
	    }
	    return strings;
	  }

//	  public Object toArray() {
//	    Object result = Array.newInstance(valueClass, values.length);
//	    for (int i = 0; i < values.length; i++) {
//	      Array.set(result, i, values[i]);
//	    }
//	    return result;
//	  }
//
//	  public void set(Writable[] values) { this.values = values; }
//
	  public List<Integer> get() { return neighborList; }

	  public void readFields(DataInput in) throws IOException {
	    int size = in.readInt();          // construct values
	    neighborList = Lists.newArrayList();
	    for (int i = 0; i < size; i++) {
	      neighborList.add(in.readInt());
	    }
	  }

	  public void write(DataOutput out) throws IOException {
	    out.writeInt(neighborList.size());                 // write values
	    for (int i = 0; i < neighborList.size(); i++){
	      out.writeInt(neighborList.get(i));
	    }
	  }
}
