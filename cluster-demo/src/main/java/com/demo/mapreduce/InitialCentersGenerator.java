package com.demo.mapreduce;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
* @ClassName: InitialCentersGenerator 
* @Description: 随机生成K个聚类中心
* @author xuechen
* @date 2017年3月17日 下午3:27:36
*  
*/
public class InitialCentersGenerator {

	public static class CentersMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		
		private Set<Long> docIdSet = new HashSet<>();
		private Text valueInfo = new Text();
		
		/**
		 * 读取初始聚类中心的docId
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			DataInputStream dis = null;
			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(caches == null && caches.length <= 0) {
				System.out.println("No Cache DocId File");
				System.exit(1);;
			}
			dis = new DataInputStream(new FileInputStream(caches[0].toString()));
			long id = 0L;
			while(dis.available() > 0) {
				id = dis.readLong();
				docIdSet.add(id);
			}
		}

		/**
		 * 输入：key为行偏移量， value为docId	docVectors
		 * 输入: key为null，value为文档向量
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			long id = Long.parseLong(str[0]);
			
			//获取初始中心id的向量
			if(docIdSet.contains(id)) {
				StringBuilder sb = new StringBuilder();
				sb.append(str[1]);
				for(int i=2; i<str.length; i++) {
					sb.append("\t").append(str[i]);
				}
				valueInfo.set(sb.toString());
				context.write(NullWritable.get(), valueInfo);
			}
		}
	}
}
