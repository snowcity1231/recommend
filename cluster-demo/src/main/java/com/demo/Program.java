package com.demo;

import javax.servlet.jsp.jstl.core.Config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.demo.mapreduce.DocCounts;
import com.demo.mapreduce.TFCount;
import com.demo.mapreduce.TFCount.TfCountsMapper;
import com.demo.mapreduce.TFCount.TfCountsReducer;
import com.demo.mapreduce.TFIDFCount.TFIDFCountMapper;
import com.demo.mapreduce.TFIDFCount.TFIDFCountReducer;
import com.demo.mapreduce.TFIDFCount;
import com.demo.mapreduce.DocCounts.DocCountsCombiner;
import com.demo.mapreduce.DocCounts.DocCountsMapper;
import com.demo.mapreduce.DocCounts.DocCountsReducer;
import com.demo.mapreduce.WordCountInDoc;
import com.demo.mapreduce.WordCountInDoc.WordCountInDocMapper;
import com.demo.mapreduce.WordCountInDoc.WordCountInDocReducer;
import com.demo.tools.CommonUtils;

/** 
* @ClassName: Programe 
* @Description: map-reduce主程序
* @author xuechen
* @date 2017年2月21日 下午4:37:22
*  
*/
public class Program {
	
	private static Log log = LogFactory.getLog(Process.class);
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//输入文件路径
		Path inputPath = new Path(args[0]);
		//最终结果输出路径
		Path outputPath = new Path(args[1]);
		
		//中间文件目录
		String tmpPath = args[2];
		Path docCountPath = new Path(tmpPath + "/0-doc-count");		//文档总数路径
		Path wordCountInDocPath = new  Path(tmpPath + "/1-word-count-in-doc");	//单词在文档中出现次数
		Path wordTfPath = new Path(tmpPath + "/2-word-tf");		//每篇文档中单词tf路径
		Path wordTFIDFPath = new Path(tmpPath + "/3-word-tfidf");	//每篇文档中每个单词的tfidf路径
		
		//删除输出目录
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		//删除临时目录
		if(fs.exists(docCountPath)) {
			fs.delete(docCountPath, true);
		}
		if(fs.exists(wordCountInDocPath)) {
			fs.delete(wordCountInDocPath, true);
		}
		if(fs.exists(wordTfPath)) {
			fs.delete(wordTfPath, true);
		}
		if(fs.exists(wordTFIDFPath)) {
			fs.delete(wordTFIDFPath, true);
		}
		
		//1、计算文档总数
		Configuration conf1 = new Configuration();
		Job countDocSumJob = new Job(conf1, "统计文档总数");
		countDocSumJob.setJarByClass(DocCounts.class);
		countDocSumJob.setInputFormatClass(SequenceFileInputFormat.class);
		countDocSumJob.setMapperClass(DocCountsMapper.class);
		countDocSumJob.setCombinerClass(DocCountsCombiner.class);
		countDocSumJob.setReducerClass(DocCountsReducer.class);
		countDocSumJob.setNumReduceTasks(1);	// 设置一个Reduce节点，只生成一个文件
		countDocSumJob.setMapOutputKeyClass(LongWritable.class);
		countDocSumJob.setMapOutputValueClass(LongWritable.class);
		countDocSumJob.setOutputKeyClass(NullWritable.class);
		countDocSumJob.setOutputValueClass(LongWritable.class);
		countDocSumJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(countDocSumJob, inputPath);
		FileOutputFormat.setOutputPath(countDocSumJob, docCountPath);
		
		countDocSumJob.waitForCompletion(true);
		
		//2、统计单词在文档中出现次数
		Configuration conf2 = new Configuration();
		Job countWordInDocJob = new Job(conf2, "单词在文档中出现次数");
		countWordInDocJob.setJarByClass(WordCountInDoc.class);
		countWordInDocJob.setMapperClass(WordCountInDocMapper.class);
		countWordInDocJob.setCombinerClass(WordCountInDocReducer.class);
		countWordInDocJob.setReducerClass(WordCountInDocReducer.class);
		countWordInDocJob.setOutputKeyClass(Text.class);
		countWordInDocJob.setOutputValueClass(IntWritable.class);
		countWordInDocJob.setInputFormatClass(SequenceFileInputFormat.class);
		countWordInDocJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(countWordInDocJob, inputPath);
		FileOutputFormat.setOutputPath(countWordInDocJob, wordCountInDocPath);
		
		countWordInDocJob.waitForCompletion(true);
		
		//3、计算文档中每个单词与该文档单词总数的占比
		Configuration conf3 = new Configuration();
		Job countWordTfJob = new Job(conf3, "每个单词的tf值");
		countWordTfJob.setJarByClass(TFCount.class);
		countWordTfJob.setMapperClass(TfCountsMapper.class);
		countWordTfJob.setReducerClass(TfCountsReducer.class);
		countWordTfJob.setOutputKeyClass(Text.class);
		countWordTfJob.setOutputValueClass(Text.class);
		countWordTfJob.setInputFormatClass(TextInputFormat.class);
		countWordTfJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(countWordTfJob, wordCountInDocPath);
		FileOutputFormat.setOutputPath(countWordTfJob, wordTfPath);
		
		countWordTfJob.waitForCompletion(true);
		
		//4、计算每个单词在各个文档的tfidf值
		Configuration conf4 = new Configuration();
		//从第一步的结果中获取文档总数，放到缓存中
		String docCountFile = docCountPath + "/part-r-00000"; 
		final Long totalDocNum = CommonUtils.getCounts(docCountFile);
		log.info("---------------------------共有" + totalDocNum + "个文档-------------------------------");
		conf4.setLong("totalDocNum", totalDocNum);
		Job countWordTFIDFJob = new Job(conf4, "计算每个单词在文章中的TFIDF");
		countWordTFIDFJob.setJarByClass(TFIDFCount.class);
		countWordTFIDFJob.setMapperClass(TFIDFCountMapper.class);
		countWordTFIDFJob.setReducerClass(TFIDFCountReducer.class);
		countWordTFIDFJob.setMapOutputKeyClass(Text.class);
		countWordTFIDFJob.setOutputValueClass(Text.class);
		countWordTFIDFJob.setOutputKeyClass(Text.class);
		countWordTFIDFJob.setOutputValueClass(DoubleWritable.class);
		countWordTFIDFJob.setInputFormatClass(TextInputFormat.class);
		countWordTFIDFJob.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(countWordTFIDFJob, wordTfPath);
		TextOutputFormat.setOutputPath(countWordTFIDFJob, wordTFIDFPath);
	
}

}
