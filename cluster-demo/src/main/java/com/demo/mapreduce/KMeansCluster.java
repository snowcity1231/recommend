package com.demo.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.demo.pojo.DataPro;
import com.demo.tools.Corpus;

import javafx.scene.chart.PieChart.Data;

/** 
* @ClassName: KMeansCluster 
* @Description: 聚类算法
* @author xuechen
* @date 2017年3月17日 下午4:56:13
*  
*/
public class KMeansCluster {

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, DataPro> {
		
		private static Log log = LogFactory.getLog(KMeansMapper.class);
		private double[][] centers;
		private int dimention_m ;	//即k值
		private int dimention_n;	//维度

		/**
		 * 读取聚类中心文件
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, DataPro>.Context context)
				throws IOException, InterruptedException {
			Path[] caches = DistributedCache.getLocalCacheArchives(context.getConfiguration());
			if(caches == null || caches.length <= 0) {
				log.error("center file does not exsit!");
				System.exit(1);
			}
			log.info("----------cache:" + caches[0].getName());
			BufferedReader br = new BufferedReader(new FileReader(caches[0].getName()));
			//所有聚类中心向量值列表
			List<List<Double>> tempCenters = new ArrayList<>();
			//单个聚类中心向量值列表
			List<Double> center = null;
			String line;
			while((line = br.readLine()) != null) {
				center = new ArrayList<>();
				String[] str = line.trim().split("\t");
				for(String s : str) {
					center.add(Double.parseDouble(s));
				}
				tempCenters.add(center);
			}
			br.close();
			//聚类中心列表转数组
			@SuppressWarnings("unchecked")
			ArrayList<Double>[] newCenters = tempCenters.toArray(new ArrayList[] {});
			//聚类数
			dimention_m = tempCenters.size();
			//特征数，即每个向量的维度
			dimention_n = newCenters[0].size();
			//一个二维数组，记录聚类中心的值
			centers = new double[dimention_m][dimention_n];
			for(int i = 0; i < dimention_m; i++) {
				//将第i个聚类中心数值列表转数组
				Double[] templeDouble = newCenters[i].toArray(new Double[] {});
				for(int j=0; j < dimention_n; j++) {
					centers[i][j] = templeDouble[j];
				}
			}
		}

		/**
		 * 输入：key为行偏移量，value为docId	docVectors
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, DataPro>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().trim().split("\t");	//[docId, docVectors]
			//文章向量存储到数组中
			double[] tempDouble = new double[strs.length - 1];	//[0.0, 0.0, 0.28 ...]
			StringBuilder sb = new StringBuilder();
			//strs[0]为docId
			for(int i=0; i<tempDouble.length; i++) {
				tempDouble[i] = Double.parseDouble(strs[i+1]);
				sb.append(strs[i+1]).append("\t");
			}
			//设置该文档所属聚类
			double distance = Double.MAX_VALUE;
			double tempDistance = 0.0D;
			int clusterId = 0;
			//遍历每一个初聚类中心，获取与当前文档距离最小的聚类，即当前文档所属聚类
			for(int i=0; i<dimention_m; i++) {
				double[] tempCenter = centers[i];	//聚类i的中心
				tempDistance = Corpus.getEuclideanDistance(tempCenter, tempDouble);
				if(tempDistance < distance) {
					clusterId = i;
					distance = tempDistance;
				}
			}
			//输出key为聚类ID，value为文档数组
			DataPro newValue = new DataPro(new Text(sb.toString()), new IntWritable(1));
			context.write(new IntWritable(clusterId), newValue);
		}
	}
	
	public static class KmeansCombiner extends Reducer<IntWritable, DataPro, IntWritable, DataPro> {
		
		@Override
		protected void reduce(IntWritable key, Iterable<DataPro> values,
				Reducer<IntWritable, DataPro, IntWritable, DataPro>.Context context)
				throws IOException, InterruptedException {
			
			int dimension = context.getConfiguration().getInt("docDimension", 0);	//获取维度
			double[] sum = new double[dimension];
			//计算同一聚类下的，每个维度的值总和
			int sumCount = 0;
			for(DataPro val : values) {
				String[] datastr = val.getCenterSum().toString().trim().split("\t");
				sumCount += val.getCount().get();
				for(int i=0; i<datastr.length; i++) {
					sum[i] += Double.parseDouble(datastr[i]);
				}
			}
			StringBuilder sb = new StringBuilder();
			for(int i=0; i<dimension; i++) {
				sb.append(sum[i] + "\t");
			}
			DataPro newValue = new DataPro(new Text(sb.toString()), new IntWritable(sumCount));
			context.write(key, newValue);
		}
	}
	
	public static class KmeansReducer extends Reducer<IntWritable, DataPro, NullWritable, Text> {

		@Override
		protected void reduce(IntWritable key, Iterable<DataPro> values,
				Reducer<IntWritable, DataPro, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			int dimension = context.getConfiguration().getInt("docDimension", 0);
			double[] sum = new double[dimension];
			int sumCount = 0;
			//计算该聚类下，每个维度值的综合
			//TODO 因为KmeansCombine里已经统计过一次，因此此处DataPro应该只有一个值，即每个维度的总和
			for(DataPro val : values) {
				String[] datastr = val.getCenterSum().toString().trim().split("\t");
				sumCount += val.getCount().get();
				for(int i=0; i < dimension; i++) {
					sum[i] += Double.parseDouble(datastr[i]);
				}
			}
			//计算平均值，获取新的聚类中心
			StringBuilder sb = new StringBuilder();
			for(int i=0; i < dimension; i++) {
				sb.append(sum[i] / sumCount).append("\t");
			}
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
	}
	
	/*
	 * 样本中心收敛（或停止迭代）后，给各网页标号
	 */
	public static class KmeansLastMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		
		private static Log log = LogFactory.getLog(KmeansLastMapper.class);
		
		private int dimention_m;	//聚类数
		private int dimention_n;	//特征词数，即维度
		private double[][] centers;

		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Path[] caches = DistributedCache.getFileClassPaths(context.getConfiguration());
			if (caches == null || caches.length <= 0) {
				log.error("聚类中心文件不存在");
				System.exit(1);
			}
			BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
			List<ArrayList<Double>> tmpCenters = new ArrayList<>();
			ArrayList<Double> center = null;
			String line;
			while((line = br.readLine()) != null) {
				center = new ArrayList<>();
				String[] str = line.trim().split("\t");
				for(int i=0; i<str.length; i++) {
					center.add(Double.parseDouble(str[i]));
				}
				tmpCenters.add(center);
			}
			br.close();
			//将所有聚类中心转为二维数组
			@SuppressWarnings("unchecked")
			ArrayList<Double>[] newCenters = tmpCenters.toArray(new ArrayList[] {});
			dimention_m = tmpCenters.size();
			dimention_n = newCenters[0].size();
			for(int i = 0; i < dimention_m; i++) {
				Double[] tmpDouble = newCenters[i].toArray(new Double[]{});
				for(int j=0; j < dimention_n; i++) {
					centers[i][j] = tmpDouble[j];
				}
			}
		}

		/**
		 * 输入：key为行偏移量，value为docId	docVectors
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().trim().split("\t");
			LongWritable docId = new LongWritable(Long.parseLong(strs[0]));
			double[] tmpDouble = new double[strs.length - 1];
			for(int i=0; i<tmpDouble.length; i++) {
				tmpDouble[i] = Double.parseDouble(strs[i+1]);
			}
			//判断属于哪一个聚类
			double distance = Double.MAX_VALUE;
			double tmpDistance = 0.0D;
			int clusterId = 0;
			for(int i=0; i < dimention_m; i++) {
				double[] tmpCenter = centers[i];
				tmpDistance = Corpus.getEuclideanDistance(tmpCenter, tmpDouble);
				if(tmpDistance < distance) {
					clusterId = i;
					distance = tmpDistance;
				}
			}
			context.write(docId, new IntWritable(clusterId));
		}
		
	}
}
