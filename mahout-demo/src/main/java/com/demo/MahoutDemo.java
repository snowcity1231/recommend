package com.demo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/** 
* @ClassName: MahoutDemo 
* @Description: 使用Mahout实现协同过滤，并获取模型评分、查准率以及召回率
* @author xuechen
* @date 2017年2月13日 下午4:07:32
*  
*/
public class MahoutDemo {

	public static void main(String[] args) throws IOException, TasteException {
		File modelFile = new File("intro.csv");

		DataModel model = new FileDataModel(modelFile);

		//用户相似度，使用基于皮尔逊相关系数计算相似度
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);

		//选择邻居用户，使用NearestNUserNeighborhood实现UserNeighborhood接口，选择邻近的4个用户
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(4, similarity, model);

		Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
		//对相同用户重复获得的推荐结果，可以用CachingRecommender将推荐结果缓存起来
//		Recommender cachingRecommender = new CachingRecommender(recommender);

		//给用户1推荐4个物品
		List<RecommendedItem> recommendations = recommender.recommend(1, 4);

		for (RecommendedItem recommendation : recommendations) {
		    System.out.println(recommendation);
		}
		
		
		//使用平均绝对差值获得评分
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		// 用RecommenderBuilder构建推荐引擎
		RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
		    @Override
		    public Recommender buildRecommender(DataModel dataModel) throws TasteException {
		        UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
		        UserNeighborhood neighborhood = new NearestNUserNeighborhood(4, similarity, dataModel);
		        return new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
		    }
		};
		//70%训练数据，30%测试数据
		double score = evaluator.evaluate(recommenderBuilder, null, model, 0.7, 1.0);
		System.out.println(score);
		
		
		// 计算推荐4个结果时的查准率和召回率
		RecommenderIRStatsEvaluator statsEvaluator = new GenericRecommenderIRStatsEvaluator();
		//还是用之前的RecommenderBuilder
		IRStatistics stats = statsEvaluator.evaluate(recommenderBuilder, null, model, null, 2, 
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		System.out.println(stats.getPrecision());
		System.out.println(stats.getRecall());
		
	}
	
}
