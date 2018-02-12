package org.apache.giraph.tools.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringWritable 
	implements
	WritableComparable<StringWritable>{

	String id;
	
	public StringWritable(String id){
		this.id = id;
	}
	
	public StringWritable(){}
	
	public void setId(String id){
		this.id = id;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		id = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(id);
	}

	@Override
	public int compareTo(StringWritable arg0) {
		return 0;
	}

}
