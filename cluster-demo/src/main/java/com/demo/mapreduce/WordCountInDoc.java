package com.demo.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.demo.tools.TextExtractor;
import com.demo.tools.TextSpliter;

/** 
* @ClassName: WordCountInDoc 
* @Description: 统计单词在文档中出现的次数
* @author xuechen
* @date 2017年2月21日 下午5:01:52
*  
*/
public class WordCountInDoc {
	
	/**
	 * 
	* @ClassName: WordCountInDocMapper 
	* @Description: 统计单词出现次数Mapper
	* @author xuechen
	* @date 2017年2月21日 下午5:24:50
	*
	 */
	public static class WordCountInDocMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Text wordAtDoc = new Text();
		
		private final IntWritable singleCount = new IntWritable(1);

		/**
		 * 输入:key为docId, value为文章内容
		 * 输出：key为word@docId，value为1	
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//筛选出中文
			String cnDoc = TextExtractor.getText(value.toString());
			//分词，获取所有单词
			List<String> words = TextSpliter.getTerms(cnDoc);
			for(String word : words) {
				this.wordAtDoc.set(word + "@" + key);
				context.write(this.wordAtDoc, this.singleCount);
			}
		}
	}
	
	/**
	 * 
	* @ClassName: WordCountInDocReducer 
	* @Description: 统计单词出现次数Reducer
	* @author xuechen
	* @date 2017年2月21日 下午5:28:45
	*
	 */
	public static class WordCountInDocReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		/**
		 * 输入：key为word@docId, value为1
		 * 输出: key为word@docId，value为该单词在docId对应的文章中出现的总次数
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			//每个word@docId次数+1
			for(IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}

}
