package com.demo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.demo.pojo.DataPro;

/** 
* @ClassName: KMeansCluster 
* @Description: TODO
* @author xuechen
* @date 2017年3月17日 下午4:56:13
*  
*/
public class KMeansCluster {

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, DataPro> {

		/**
		 * 读取聚类中心文件
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, DataPro>.Context context)
				throws IOException, InterruptedException {
			Path[] caches = DistributedCache.getLocalCacheArchives(context.getConfiguration());
			//TODO
		}
		
	}
}
