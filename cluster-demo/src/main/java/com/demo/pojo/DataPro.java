package com.demo.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** 
* @ClassName: DataPro 
* @Description: TODO
* @author xuechen
* @date 2017年3月17日 下午4:58:20
*  
*/
public class DataPro implements Writable{
	
	//局部样本中心
	private Text centerSum;
	//属于簇中的样本数
	private IntWritable count;

	
	/**
	 * @param centerSum
	 * @param count
	 */
	public DataPro(Text centerSum, IntWritable count) {
		this.centerSum = centerSum;
		this.count = count;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.centerSum.readFields(input);
		this.count.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		this.centerSum.write(output);
		this.count.write(output);
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
