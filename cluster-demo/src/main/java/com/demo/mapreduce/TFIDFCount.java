package com.demo.mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/** 
* @ClassName: TFIDFCounter 
* @Description: 统计单词的TFIDF
* @author xuechen
* @date 2017年2月24日 下午3:05:37
*  
*/
public class TFIDFCount {

	public static class TFIDFCountMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text word = new Text();
		private Text docIdAndCounters = new Text();

		/**
		 * 输入: key为行偏移量， value为 'word@docId a/b'，a为该word在docId对应文档中出现的次数，b为docId对应文档中单词的总数
		 * 输出:key为word，value为 docId=a/b
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] wordAndCounters = value.toString().split("\t");
			String[] wordAtDoc = wordAndCounters[0].split("@");
			this.word.set(wordAtDoc[0]);
			this.docIdAndCounters.set(wordAtDoc[1] + "=" + wordAndCounters[1]);
			context.write(this.word, this.docIdAndCounters);
		}
	}
	
	public static class TFIDFCountReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		
		private static final DecimalFormat DF = new DecimalFormat("###.########");
		private Text wordAtDoc = new Text();
		private DoubleWritable tfidfCounts = new DoubleWritable();
		
		/**
		 * 输入:key为word，value为 docId=a/b
		 * 输出: key为word@docId， value为该单词在docId对应文章中的的tfidf
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			//从配置中获取文档总数
			Long totalDocNumber = context.getConfiguration().getLong("totalDocNumber", 0);
			//包含当前单词(key)的文章数
			int docNumbersIncludeKey = 0;
			//存放该单词在每篇文章的频率，key为docId，value为TF
			Map<String, String> frequencyMap = new HashMap<>();
			for (Text val : values) {
				String[] docIdAndFrequencies = val.toString().split("=");
				String docId = docIdAndFrequencies[0];
				//单词在文章中出现的频率
				String[] wordFrequencies = docIdAndFrequencies[1].split("/");
				//单词出现的次数
				int wordNum = Integer.parseInt(wordFrequencies[0]);
				if (wordNum > 0) {
				    docNumbersIncludeKey ++;	
				}
				frequencyMap.put(docId, docIdAndFrequencies[1]);
			}
			
			//遍历所有文章，获取每篇文章下该单词的TFIDF
			for(String documentId : frequencyMap.keySet()) {
				//获取分数表示的频率
				String[] wordFrequency = frequencyMap.get(documentId).split("/");
				Double wordNumInDoc = Double.valueOf(wordFrequency[0]);	//单词出现次数
				Double totalInDoc = Double.valueOf(wordFrequency[1]);	//文档单词总次数
				//计算TF
				double tf = Double.valueOf(wordNumInDoc / totalDocNumber);
				//计算IDF
				//单词出现的此处如果为0，则取1，除数不能为0
				if(docNumbersIncludeKey == 0) {
					docNumbersIncludeKey = 1;
				}
				double idf = Math.log10((double) totalDocNumber / (double) docNumbersIncludeKey);
				double tfidf = tf * idf;
				
				this.wordAtDoc.set(key + "@" + documentId);
				this.tfidfCounts.set(Double.parseDouble(DF.format(tfidf)));
				
				context.write(this.wordAtDoc, this.tfidfCounts);
			}
		}
		
	}
}
