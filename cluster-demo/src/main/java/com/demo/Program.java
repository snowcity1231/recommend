package com.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import com.demo.mapreduce.DocCounts.DocCountsCombiner;
import com.demo.mapreduce.DocCounts.DocCountsMapper;
import com.demo.mapreduce.DocCounts.DocCountsReducer;
import com.demo.mapreduce.WordCountInDoc;
import com.demo.mapreduce.WordCountInDoc.WordCountInDocMapper;
import com.demo.mapreduce.WordCountInDoc.WordCountInDocReducer;

/** 
* @ClassName: Programe 
* @Description: map-reduce主程序
* @author xuechen
* @date 2017年2月21日 下午4:37:22
*  
*/
public class Program {
	
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
	}

}
