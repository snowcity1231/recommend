package com.demo.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/** 
* @ClassName: TfCounts 
* @Description: 统计每个单词的tf
* @author xuechen
* @date 2017年2月22日 下午2:52:04
*  
*/
public class TfCounts {

	public static class TfCountsMapper extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * 输入：key为行偏移量，value为 'word@docId 数量'
		 * 输出:key为docId, value为'word=数量'
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] wordAtDocAndCount = value.toString().split("\t");
			String count = wordAtDocAndCount[1];
			String[] wordAndDoc = wordAtDocAndCount[0].split("@");
			String word = wordAndDoc[0];
			String docId = wordAndDoc[1];
			context.write(new Text(docId), new Text(word + "=" + count));
		}
	}
	
	public static class TfCountsReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text wordAtDoc = new Text();
		private Text wordAvar = new Text();

		/**
		 * 输入:key为docId, value为'word=数量'(1 大=5)
		 * 输出:key为word@docId，value为 a/b，其中a为该word在docId对应文档中出现的次数，b为docId对应文档中单词的总数
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int sumWordsInDoc = 0;
			Map<String, Integer> wordCountMap = new HashMap<>();
			for(Text val : values) {
				String[] wordAndCount = val.toString().split("=");
				String word = wordAndCount[0];
				int count = Integer.valueOf(wordAndCount[1]);
				wordCountMap.put(word, count);
				sumWordsInDoc += count;
			}
			for(String wordKey : wordCountMap.keySet()) {
				wordAtDoc = new Text(wordKey + "@" + key);
				int wordCount = wordCountMap.get(wordKey);
				wordAvar = new Text(wordCount + "/" + sumWordsInDoc);
				context.write(wordAtDoc, wordAvar);
			}
		}
		
	}
}
