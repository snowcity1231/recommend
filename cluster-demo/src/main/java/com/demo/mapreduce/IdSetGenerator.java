package com.demo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
* @ClassName: IdSetGenerator 
* @Description: TODO
* @author xuechen
* @date 2017年3月17日 下午2:45:40
*  
*/
public class IdSetGenerator {

	public static class IdSetGeneratorMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

		/**
		 * 输入：key为行偏移量，value为docId	docVector
		 * 输出: key为docId, value为Null
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			Long id = Long.parseLong(str[0]);
			context.write(new LongWritable(id), NullWritable.get());
		}
		
	}
}
