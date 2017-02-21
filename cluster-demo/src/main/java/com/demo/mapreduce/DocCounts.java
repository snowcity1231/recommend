package com.demo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/** 
* @ClassName: DocCounts 
* @Description: 统计文档总数
* @author xuechen
* @date 2017年2月21日 下午4:22:38
*  
*/
public class DocCounts {
	
	public static class DocCountsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		
		private final LongWritable one = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//每统计一篇文档，计数+1
			context.write(one, one);
		}
		
	}
	
	public static class DocCountsCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values,
				Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
			for(LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
		
	}
	
	public static class DocCountsReducer extends Reducer<LongWritable, LongWritable, NullWritable, LongWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values,
				Reducer<LongWritable, LongWritable, NullWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(null, new LongWritable(sum));
		}
		
	}

}
