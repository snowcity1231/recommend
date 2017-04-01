package com.demo.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DataPro implements Writable {

	private Text centerSum; //局部样本中心(累和sum)
	private IntWritable count;		//属于簇中的样本数
	
	
	public DataPro(){
		this.centerSum = new Text();
		this.count = new IntWritable();
	}
	/**
	 * @param vec
	 * @param count
	 */
	public DataPro(Text centerSum, IntWritable count) {
		super();
		this.centerSum = centerSum;
		this.count = count;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.centerSum.readFields(arg0);
		this.count.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.centerSum.write(arg0);
		this.count.write(arg0);
	}

	/**
	 * @return the centerSum
	 */
	public Text getCenterSum() {
		return centerSum;
	}

	/**
	 * @param centerSum the centerSum to set
	 */
	public void setCenterSum(Text centerSum) {
		this.centerSum = centerSum;
	}

	/**
	 * @return the count
	 */
	public IntWritable getCount() {
		return count;
	}

	/**
	 * @param count the count to set
	 */
	public void setCount(IntWritable count) {
		this.count = count;
	}
}
