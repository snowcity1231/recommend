package com.demo.tools;


import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** 
* @ClassName: Corpus 
* @Description: TODO
* @author xuechen
* @date 2017年3月17日 下午3:07:06
*  
*/
public class Corpus {
	
	/**
	 * 生成初始化聚类中心
	 * @param k 聚类中心数
	 * @param totalDocNum	  文档总数
	 * @param inputFileName	  hdfs上所有文档ID集合文件名   /6-chinese-docid-set/part-r-00000
	 * @param outputFileName   生成文件路径	/7-0-initial-docid
	 * @throws IOException 
	 */
	public static void generateKInitialCenters(int k, Long totalDocNum, String inputFileName, String outputFileName) throws IOException {
		Configuration conf1 = new Configuration();
		FileSystem fs1 = FileSystem.get(URI.create(inputFileName), conf1);
		Configuration conf2 = new Configuration();
		FileSystem fs2 = FileSystem.get(URI.create(outputFileName), conf2);
		
		FSDataInputStream in = null;
		FSDataOutputStream out = null;
		
		// 获取文档ID集合
		in = fs1.open(new Path(inputFileName));
		Set<Long> docIdSet = new HashSet<>();
		String line;
		while((line = in.readLine()) != null) {
			docIdSet.add(Long.parseLong(line));
		}
		in.close();
		
		out = fs2.create(new Path(outputFileName));
		Set<Long> randomDocId = new HashSet<>();
		int i = 0;
		while(i < k) {
			long rId = Math.round(Math.random() * totalDocNum);
			if(docIdSet.contains(rId) && !randomDocId.contains(rId)) {
				randomDocId.add(rId);
				i++;
			}
		}
		for(Long docId : randomDocId) {
			out.writeLong(docId);
		}
		out.close();
	}
	
	/**
	 * 
	 * @param path HDFS上初始中心文件的位置
	 * @return
	 * @throws IOException
	 */
	public static int getDimension(String path) throws IOException {
		int dimension = 0;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		FSDataInputStream in = null;
		
		in = fs.open(new Path(path));
		String line;
		while((line = in.readLine()) != null) {
			String[] str = line.trim().split("\t");
			dimension = str.length;
			break;
		}
		in.close();
		return dimension;
	}

}
