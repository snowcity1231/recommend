package com.demo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/** 
* @ClassName: VocabularyFilter 
* @Description: 根据TFIDF筛选关键词
* @author xuechen
* @date 2017年3月10日 下午3:58:00
*  
*/
public class VocabularySelector {

	public static class VocabularyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		private Text word = new Text();
		private DoubleWritable tfidf = new DoubleWritable();
		
		/**
		 * 输入：key为行偏移量，value为'word@docId tfidf'
		 * 输出: key为word， value为tfidf
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] wordAtDocAndTFIDF = value.toString().split("\t");
			String[] wordAtDoc = wordAtDocAndTFIDF[0].split("@");
			word.set(wordAtDoc[0]);
			tfidf.set(Double.parseDouble(wordAtDocAndTFIDF[1]));
			
			context.write(word, tfidf);
		}
	}
	
	public static class VocabularyCombine extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		private DoubleWritable maxTfidf = new DoubleWritable();

		/**
		 * 输入: key为word， value为tfidf
		 * 输出：key为word，value为该单词在所有文章中的最大tfidf
		 */
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double max = 0.0d;
			for(DoubleWritable value : values) {
				if(value.get() > max) {
					max = value.get();
				}
			}
			maxTfidf.set(max);
			context.write(key, maxTfidf);
		}
	}
	
	public static class VocabularyReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {

		/**
		 * 输入：key为word，value为该单词的maxTFIDF最大tfidf
		 * 输出：key为经筛选后保留下的word，value为null
		 */
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//设定tfidf阙值
			final double THRESHOLD = 0.007D;
			
			//保留tfidf大于阙值的单词
			for(DoubleWritable value : values) {
				if(value.get() >= THRESHOLD) {
					context.write(key, NullWritable.get());
					break;
				}
			}
		}
		
	}
}
