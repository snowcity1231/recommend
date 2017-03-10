package com.demo.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** 
* @ClassName: DocVector 
* @Description: 文档向量Bean
* @author xuechen
* @date 2017年3月10日 下午4:48:29
*  
*/
public class DocVector implements Writable{
	
	private long docId;
	private double[] tfidf;
	
	public DocVector() {
	}

	/**
	 * @param docId
	 * @param tfidf
	 */
	public DocVector(long docId, double[] tfidf) {
		this.docId = docId;
		this.tfidf = tfidf;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String[] str = in.readUTF().split("\t");
		this.docId = Long.parseLong(str[0]);
		this.tfidf = new double[str.length - 1];
		for(int i=0; i<this.tfidf.length; i++) {
			this.tfidf[i] = Double.parseDouble(str[i+1]);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.toString());
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(String.valueOf(this.docId));
		for(int i=0; i<this.tfidf.length; i++) {
			sb.append("\t" + String.valueOf(this.tfidf[i]));
		}
		return sb.toString();
	}

	/**
	 * @return the docId
	 */
	public long getDocId() {
		return docId;
	}

	/**
	 * @param docId the docId to set
	 */
	public void setDocId(long docId) {
		this.docId = docId;
	}

	/**
	 * @return the tfidf
	 */
	public double[] getTfidf() {
		return tfidf;
	}

	/**
	 * @param tfidf the tfidf to set
	 */
	public void setTfidf(double[] tfidf) {
		this.tfidf = tfidf;
	}

}
