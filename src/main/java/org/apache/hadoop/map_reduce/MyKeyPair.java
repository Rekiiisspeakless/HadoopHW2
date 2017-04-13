package org.apache.hadoop.map_reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class MyKeyPair implements WritableComparable<MyKeyPair> {
	private int i;
	private int k;
	public MyKeyPair(){
		this.i = 0;
		this.k = 0;
	}
	public MyKeyPair(int i, int k){
		this.i = i;
		this.k = k;
		//this.i.set(i);
		//this.k.set(k);
	}
	public  void set(int i, int k){
		this.i = i;
		this.k = k;
	}
	public int getI(){
		return i;
	}
	public int getK(){
		return k;
	}
	public void write(DataOutput out) throws IOException {
	 out.writeInt(i);
	 out.writeInt(k);
   }
   
   public void readFields(DataInput in) throws IOException {
	 i = in.readInt();
	 k = in.readInt();
   }


   public static  MyKeyPair read(DataInput in) throws IOException{
		MyKeyPair  myKeyPair = new MyKeyPair();
		myKeyPair.readFields(in);
		return  myKeyPair;
   }


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		MyKeyPair myKeyPair = (MyKeyPair) o;

		if (getI() != myKeyPair.getI()) return false;
		return getK() == myKeyPair.getK();
	}

	@Override
	public int hashCode() {
		int result = getI();
		result = 31 * result + getK();
		return result;
	}

	@Override
    public int compareTo(MyKeyPair o) {
	    /*int value1 = this.i - o.getI();
	    int value2 = this.k - o.getK();
	    if(value1 != 0){
	    	return  value1;
		}else{
	    	return value2;
		}*/
		if (Integer.compare(this.getI(), o.getI()) != 0) {
			return Integer.compare(this.getI(), o.getI());
		} else {
			return Integer.compare(this.getK(), o.getK());
		}
    }

	@Override
	public String toString() {
		return "MyKeyPair{" +
				"i=" + i +
				", k=" + k +
				'}';
	}
}