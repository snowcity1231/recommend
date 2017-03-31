package com.demo.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.demo.pojo.DocVector;

/** 
* @ClassName: GenerateDocVectors 
* @Description: 生成文档向量
* @author xuechen
* @date 2017年3月10日 下午4:40:06
*  
*/
public class GenerateDocVectors {

	public static class DocVectorsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private LongWritable docId = new LongWritable();
		private Text wordAndTfidf = new Text();

		/**
		 * 输入:key为行偏移量，value为 word@docId tfidf
		 * 输出:key为docId, value为 word=tfidf
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] wordAtDocidAndTfidf = value.toString().split("\t");
			String[] wordAtDoc = wordAtDocidAndTfidf[0].split("@");
			docId.set(Long.parseLong(wordAtDoc[1]));
			wordAndTfidf.set(wordAtDoc[0] + "=" + wordAtDocidAndTfidf[1]);
			context.write(docId, wordAndTfidf);
		}
	}
	
	public static class DocVectorReducer extends Reducer<LongWritable, Text, NullWritable, DocVector> {
		
		private List<String> keywordList = new ArrayList<>();
		private DocVector docVector = null;
		
		/**
		 * 读取关键字列表文件
		 */
		@Override
		protected void setup(Reducer<LongWritable, Text, NullWritable, DocVector>.Context context)
				throws IOException, InterruptedException {
			BufferedReader br = null;
			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(caches == null || caches.length <= 0) {
				System.out.println("No DistributedCach keywords File");
				System.exit(1);
			}
			br = new BufferedReader(new FileReader(caches[0].toString()));
			
			String line;
			while((line = br.readLine()) != null) {
				if(StringUtils.isNotEmpty(line.trim())) {
					keywordList.add(line);
				}
			}
			br.close();
		}

		/**
		 * 输入：key为docId, value为 word=tfidf
		 * 输出：key为docId，value为文档向量
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, NullWritable, DocVector>.Context context)
				throws IOException, InterruptedException {
			
			//key为单词，value为tfidf
			HashMap<String, Double> tfidfMap = new HashMap<>();
			for(Text value : values) {
				String[] wordAndTfidf = value.toString().split("=");
				tfidfMap.put(wordAndTfidf[0], Double.valueOf(wordAndTfidf[1]));
			}
			
			//TFIDF向量列表
			List<Double> tfidfVectors = new ArrayList<>();
			
			//遍历关键字列表
			for(String keyword : keywordList) {
				if(tfidfMap.containsKey(keyword)) {
					tfidfVectors.add(tfidfMap.get(keyword));
				}else {
					tfidfVectors.add(0.0d);
				}
			}
			//向量列表转数组
			double[] arr = new double[keywordList.size()];
			for(int i=0; i<arr.length; i++) {
				arr[i] = tfidfVectors.get(i);
			}
			docVector = new DocVector(key.get(), arr);
			context.write(NullWritable.get(), docVector);
		}
		
	}
}
