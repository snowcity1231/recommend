package com.demo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

/** 
* @ClassName: SlopeOneDemo 
* @Description: TODO
* @author xuechen
* @date 2017年2月14日 上午10:10:43
*  
*/
public class SlopeOneDemo {

	public static void main(String[] args) throws Exception {
		DataModel model = new FileDataModel(new File("data.txt"));
		Recommender recommender = new SlopeOneRecommender(model);
		
		List<RecommendedItem> recommendations = recommender.recommend(1, 4);
		for(RecommendedItem recommendation : recommendations) {
			System.out.println(recommendation);
		}
	}
}
