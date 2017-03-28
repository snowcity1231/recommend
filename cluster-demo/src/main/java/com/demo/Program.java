package com.demo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.demo.mapreduce.DocCounts;
import com.demo.mapreduce.DocCounts.DocCountsCombiner;
import com.demo.mapreduce.DocCounts.DocCountsMapper;
import com.demo.mapreduce.DocCounts.DocCountsReducer;
import com.demo.mapreduce.GenerateDocVectors;
import com.demo.mapreduce.GenerateDocVectors.DocVectorReducer;
import com.demo.mapreduce.GenerateDocVectors.DocVectorsMapper;
import com.demo.mapreduce.IdSetGenerator;
import com.demo.mapreduce.IdSetGenerator.IdSetGeneratorMapper;
import com.demo.mapreduce.InitialCentersGenerator;
import com.demo.mapreduce.InitialCentersGenerator.CentersMapper;
import com.demo.mapreduce.KMeansCluster.KmeansCombiner;
import com.demo.mapreduce.KMeansCluster.KmeansReducer;
import com.demo.mapreduce.KMeansCluster;
import com.demo.mapreduce.TFCount;
import com.demo.mapreduce.TFCount.TfCountsMapper;
import com.demo.mapreduce.TFCount.TfCountsReducer;
import com.demo.mapreduce.TFIDFCount;
import com.demo.mapreduce.TFIDFCount.TFIDFCountMapper;
import com.demo.mapreduce.TFIDFCount.TFIDFCountReducer;
import com.demo.mapreduce.VocabularySelector;
import com.demo.mapreduce.VocabularySelector.VocabularyCombine;
import com.demo.mapreduce.VocabularySelector.VocabularyMapper;
import com.demo.mapreduce.VocabularySelector.VocabularyReducer;
import com.demo.mapreduce.WordCountInDoc;
import com.demo.mapreduce.WordCountInDoc.WordCountInDocMapper;
import com.demo.mapreduce.WordCountInDoc.WordCountInDocReducer;
import com.demo.pojo.DataPro;
import com.demo.pojo.DocVector;
import com.demo.tools.CommonUtils;
import com.demo.tools.Corpus;

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
		Path docCountPath = new Path(tmpPath + "/1-doc-count");		//文档总数路径
		Path wordCountInDocPath = new  Path(tmpPath + "/2-word-count-in-doc");	//单词在文档中出现次数
		Path wordTfPath = new Path(tmpPath + "/3-word-tf");		//每篇文档中单词tf路径
		Path wordTFIDFPath = new Path(tmpPath + "/4-word-tfidf");	//每篇文档中每个单词的tfidf路径
		Path vocabularyPath = new Path(tmpPath + "/5-vocabulary");	//存放筛选结束的单词列表文件路径
		Path docVectorsPath = new Path(tmpPath + "/6-doc-vectors");  //存放文档向量文件路径
		Path docIdSetPath = new Path(tmpPath + "/7-docid-set");	//文档id集合
		Path initialCenterPath = new Path(tmpPath + "/8-initial-centers");	//初始化聚类中心
		final String tmpCenter = tmpPath + "/9-tmp-centers/";
		Path tmpCentersPath = new Path(tmpCenter);
		
		//生成聚类数
		final int K = Integer.parseInt(args[3]);
		//最大迭代次数
		final int MAX_ITERATIONS = Integer.parseInt(args[4]);
		
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
		if(fs.exists(vocabularyPath)) {
			fs.delete(vocabularyPath, true);
		}
		if(fs.exists(docVectorsPath)) {
			fs.delete(docVectorsPath, true);
		}
		if(fs.exists(docIdSetPath)) {
			fs.delete(docIdSetPath, true);
		}
		if(fs.exists(initialCenterPath)) {
			fs.delete(initialCenterPath, true);
		}
		if(fs.exists(tmpCentersPath)) {
			fs.delete(tmpCentersPath, true);
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
		
		//5、根据单词的TFIDF值筛选单词，用于生成向量
		Configuration conf5 = new Configuration();
		Job vocabularySelectorJob = new Job(conf5, "筛选单词");
		vocabularySelectorJob.setJarByClass(VocabularySelector.class);
		vocabularySelectorJob.setInputFormatClass(TextInputFormat.class);
		vocabularySelectorJob.setMapperClass(VocabularyMapper.class);
		vocabularySelectorJob.setMapOutputKeyClass(Text.class);
		vocabularySelectorJob.setMapOutputValueClass(DoubleWritable.class);
		vocabularySelectorJob.setCombinerClass(VocabularyCombine.class);
		vocabularySelectorJob.setReducerClass(VocabularyReducer.class);
		vocabularySelectorJob.setNumReduceTasks(1);
		vocabularySelectorJob.setOutputKeyClass(Text.class);
		vocabularySelectorJob.setOutputValueClass(NullWritable.class);
		vocabularySelectorJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(vocabularySelectorJob, wordTFIDFPath);
		FileOutputFormat.setOutputPath(vocabularySelectorJob, vocabularyPath);
		
		//6、生成文档向量
		Configuration conf6 = new Configuration();
		Path vocabularyFilePath = new Path(vocabularyPath + "/part-r-00000");
		//关键词列表文件放到缓存中
		DistributedCache.addCacheFile(vocabularyFilePath.toUri(), conf6);
		Job docVectorsJob = new Job(conf6, "生成文档向量");
		docVectorsJob.setJarByClass(GenerateDocVectors.class);
		docVectorsJob.setMapperClass(DocVectorsMapper.class);
		docVectorsJob.setInputFormatClass(TextInputFormat.class);
		docVectorsJob.setOutputFormatClass(TextOutputFormat.class);
		docVectorsJob.setMapOutputKeyClass(LongWritable.class);
		docVectorsJob.setMapOutputValueClass(Text.class);
		docVectorsJob.setReducerClass(DocVectorReducer.class);
		docVectorsJob.setOutputKeyClass(NullWritable.class);
		docVectorsJob.setOutputValueClass(DocVector.class);
		FileInputFormat.addInputPath(docVectorsJob, wordTFIDFPath);
		FileOutputFormat.setOutputPath(docVectorsJob, docVectorsPath);
		
		//7、生成ID集合
		Configuration conf7 = new Configuration();
		Job idSetJob = new Job(conf7, "生成id集合");
		idSetJob.setJarByClass(IdSetGenerator.class);
		idSetJob.setMapperClass(IdSetGeneratorMapper.class);
		idSetJob.setNumReduceTasks(1);
		idSetJob.setMapOutputKeyClass(LongWritable.class);
		idSetJob.setMapOutputValueClass(NullWritable.class);
		idSetJob.setReducerClass(Reducer.class);
		idSetJob.setOutputKeyClass(LongWritable.class);
		idSetJob.setOutputValueClass(NullWritable.class);
		idSetJob.setInputFormatClass(TextInputFormat.class);
		idSetJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(idSetJob, docVectorsPath);
		FileOutputFormat.setOutputPath(idSetJob, docIdSetPath);
		
		//8、生成初始中心文件
		Configuration conf8 = new Configuration();
		//上一步生成的id集合文件
		String docIdSetFile = docIdSetPath + "/part-r-00000";
		//hdfs上存放初始文档id的文件名
		String initDocIdFileName = tmpPath + "/8-0-initial-docid";
		Corpus.generateKInitialCenters(K, totalDocNum, docIdSetFile, initDocIdFileName);
		//生成的初始文档中心文件放在缓存中
		Path docIdPath = new Path(initDocIdFileName);
		DistributedCache.addCacheFile(docIdPath.toUri(), conf8);
		Job initCentersJob = new Job(conf8, "生成初始中心文件");
		initCentersJob.setJarByClass(InitialCentersGenerator.class);
		initCentersJob.setMapOutputKeyClass(CentersMapper.class);
		initCentersJob.setNumReduceTasks(1);
		initCentersJob.setMapOutputKeyClass(NullWritable.class);
		initCentersJob.setMapOutputValueClass(Text.class);
		initCentersJob.setReducerClass(Reducer.class);
		initCentersJob.setOutputKeyClass(NullWritable.class);
		initCentersJob.setOutputValueClass(Text.class);
		initCentersJob.setInputFormatClass(TextInputFormat.class);
		initCentersJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(initCentersJob, docVectorsPath);
		FileOutputFormat.setOutputPath(initCentersJob, initialCenterPath);
		
		//9、初次聚类
		Configuration conf9 = new Configuration();
		String initialCentersFileName = initialCenterPath.toString() + "/part-r-00000";
		int dimension = Corpus.getDimension(initialCentersFileName);
		conf9.setInt("dimension", dimension);
		Path initialCentersFile = new Path(initialCentersFileName);
		DistributedCache.addCacheFile(initialCentersFile.toUri(), conf9);
		Job firstKmeansJob = new Job(conf9, "初次聚类");
		firstKmeansJob.setJarByClass(KMeansCluster.class);
		firstKmeansJob.setMapOutputKeyClass(IntWritable.class);
		firstKmeansJob.setMapOutputValueClass(DataPro.class);
		firstKmeansJob.setNumReduceTasks(1);
		firstKmeansJob.setCombinerClass(KmeansCombiner.class);
		firstKmeansJob.setReducerClass(KmeansReducer.class);
		firstKmeansJob.setOutputKeyClass(NullWritable.class);
		firstKmeansJob.setOutputValueClass(Text.class);
		firstKmeansJob.setInputFormatClass(TextInputFormat.class);
		firstKmeansJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(firstKmeansJob, docVectorsPath);
		FileOutputFormat.setOutputPath(firstKmeansJob, new Path(tmpCenter + 0 + "/"));
		if(!firstKmeansJob.waitForCompletion(true)) {
			log.error("初次聚类失败!");
			System.exit(1);
		}
		
		//TODO 开始遍历
		
		
	}

}
