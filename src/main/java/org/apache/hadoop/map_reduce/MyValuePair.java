package org.apache.hadoop.map_reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public  class MyValuePair implements Writable {
	private int name;
	private int index1;
	private int index2;
	public MyValuePair(int name, int index1, int index2){
		this.name = name;
		this.index1 = index1;
		this.index2 = index2;
	}
	public  MyValuePair(){
		this.name = 0;
		this.index1 = 0;
		this.index2 = 0;
	}
	public  void set(int name, int index1, int index2){
		this.name = name;
		this.index1 = index1;
		this.index2 = index2;
	}
	public int getName(){
		return name;
	}
	public int getIndex1(){
		return index1;
	}
	public int getIndex2(){
		return index2;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(name);
		out.writeInt(index1);
		out.writeInt(index2);
   }
   
   public void readFields(DataInput in) throws IOException {
	 this.name = in.readInt();
	 this.index1 = in.readInt();
	 this.index2 = in.readInt();
   }
   
   public static MyValuePair read(DataInput in) throws IOException {
	   MyValuePair w = new MyValuePair();
	 	w.readFields(in);
	 	return w;
   }


	@Override
	public String toString() {
		return "MyValuePair{" +
				"name=" + name +
				", index=" + index1 +
				", value=" + index2 +
				'}';
	}
}