package com.demo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.BooleanPreference;
import org.apache.mahout.cf.taste.impl.model.BooleanUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/** 
* @ClassName: RecommendDemo 
* @Description: 使用Apache Mahout实现协同过滤
* @author xuechen
* @date 2017年2月13日 下午3:57:18
*  
*/
public class RecommendDemo {

	public static void main(String[] args) throws Exception  {
//		userBaseRecommender();
//		itemBaseRecommender();
//		cfDemo();
//		userCfBasedBoolean();
		booleanUserCfBasedMemory();
	}
	
	/**
	 * 基于用户相似度的推荐
	 * @throws Exception
	 */
	public static void userBaseRecommender() throws Exception{
		// step1 构建模型 
		DataModel model = new FileDataModel(new File("data.txt"));
		//第二个参数标识是否更换userId与itemId位置，第三个参数标识重新载入时最小时间间隔
//		model = new FileDataModel(new File("data.txt"), true, 6000);
		//使用mysql模型
//		model = new MySQLJDBCDataModel(dataSource, table, "user_id", "item_id", "preference", "date");
		// step2 计算相似度 （基于皮尔逊相关系数计算相似度） 
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model); 
		// step3 查找k紧邻 
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model); 
		// 构造推荐引擎 
		Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity); 
		// 为用户1推荐两个ItemID 
		List<RecommendedItem> recommendations = recommender.recommend(1, 2); 
		for (RecommendedItem recommendation : recommendations) { 
			System.out.println("------------基于用户的推荐：" + recommendation + "-------------------"); 
		} 
	}
	
	/**
	 * 基于内容的推荐引擎
	 * @throws Exception
	 */
	public static void itemBaseRecommender() throws Exception {
		DataModel model = new FileDataModel(new File("data.txt"));
		
		//计算商品相似度
		ItemSimilarity similarity = new PearsonCorrelationSimilarity(model);
		Recommender recommender = new GenericBooleanPrefItemBasedRecommender(model, similarity);
		List<RecommendedItem> recommendations = recommender.recommend(1, 1);
		for(RecommendedItem recommendation : recommendations) {
			System.out.println("------------基于内容的推荐：" + recommendation + "-------------------");
		}
	}
	
	public static void cfDemo() throws Exception {
		//基于程序创建DataModel
		FastByIDMap<PreferenceArray> preferences = new FastByIDMap<>();
		//用户1喜欢物品1、2、3
		PreferenceArray userPref1 = new GenericUserPreferenceArray(3);
		userPref1.set(0, new GenericPreference(1, 1, 5.0f));
		userPref1.set(1, new GenericPreference(1, 2, 3.0f));
		userPref1.set(2, new GenericPreference(1, 3, 2.5f));
		// 用户2喜欢物品1、2、3、4 
		PreferenceArray userPref2 = new GenericUserPreferenceArray(4);
		userPref2.set(0, new GenericPreference(2, 1, 2.0f)); 
		userPref2.set(1, new GenericPreference(2, 2, 2.5f)); 
		userPref2.set(2, new GenericPreference(2, 3, 5.0f)); 
		userPref2.set(3, new GenericPreference(2, 4, 2.0f)); 
		// 用户3喜欢物品1、4、5、7 
		PreferenceArray userPref3 = new GenericUserPreferenceArray(4); 
		userPref3.set(0, new GenericPreference(3, 1, 2.5f)); 
		userPref3.set(1, new GenericPreference(3, 4, 4.0f)); 
		userPref3.set(2, new GenericPreference(3, 5, 4.5f)); 
		userPref3.set(3, new GenericPreference(3, 7, 5.0f)); 
		// 用户4喜欢物品1、3、4、6 
		PreferenceArray userPref4 = new GenericUserPreferenceArray(4); 
		userPref4.set(0, new GenericPreference(4, 1, 5.0f)); 
		userPref4.set(1, new GenericPreference(4, 3, 3.0f));
		userPref4.set(3, new GenericPreference(4, 6, 4.0f)); 
		// 用户5喜欢物品1、2、3、4、5、6 
		PreferenceArray userPref5 = new GenericUserPreferenceArray(6); 
		userPref5.set(0, new GenericPreference(5, 1, 4.0f)); 
		userPref5.set(1, new GenericPreference(5, 2, 3.0f)); 
		userPref5.set(2, new GenericPreference(5, 3, 2.0f)); 
		userPref5.set(3, new GenericPreference(5, 4, 4.0f)); 
		userPref5.set(4, new GenericPreference(5, 5, 3.5f)); 
		userPref5.set(5, new GenericPreference(5, 6, 4.0f)); 
		preferences.put(1, userPref1); 
		preferences.put(2, userPref2); 
		preferences.put(3, userPref3); 
		preferences.put(4, userPref4); 
		preferences.put(5, userPref5); 
		//构建Datamodel
		DataModel model = new GenericDataModel(preferences);
		//基于欧式距离计算用户相似度
		UserSimilarity similarity = new EuclideanDistanceSimilarity(model);
		//查找最相邻的2个用户
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model);
		Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
		//输出用户4的用户信息
		List<RecommendedItem> recommendations = recommender.recommend(4, 3);
		for(RecommendedItem recommendation : recommendations) { 
			System.out.println("------------用户4的推荐信息：" + recommendation + "-------------------");
		}
		
		//输出所有用户的推荐信息
		System.out.println("------------所有用户的推荐信息：-------------------");
		LongPrimitiveIterator iter = model.getUserIDs();
		while(iter.hasNext()) {
			long uid = iter.nextLong();
			List<RecommendedItem> list = recommender.recommend(uid, 3);
			System.out.printf("uid:%s", uid);
			for(RecommendedItem rItem : list) {
				System.out.printf("(%s,%f)", rItem.getItemID(), rItem.getValue());
			}
			System.out.println();
		}
	}
	
	/**
	 * 基于布尔类型的喜好
	 * @throws TasteException
	 * @throws IOException
	 */
	public static void userCfBasedBoolean() throws TasteException, IOException {
		DataModel dataModel = new FileDataModel(new File("data_nopref.csv"));
		 
        //用户相识度 ：皮尔森相关性相视度
        //UserSimilarity sim = new PearsonCorrelationSimilarity(model);
 
        //用户相识度 ：欧式距离
        UserSimilarity sim = new LogLikelihoodSimilarity(dataModel);
 
        // 最近邻算法
        UserNeighborhood nbh = new NearestNUserNeighborhood(4, sim, dataModel);
        
		for(int i=1; i<=5; i++) {
			long[] ids = nbh.getUserNeighborhood(i);
			StringBuilder idSb = new StringBuilder();
			for(long id : ids) {
				idSb.append(id + "(" + sim.userSimilarity(i, id) + ")" + ",");
			}
			System.out.println("用户" + i + "的相邻用户：" + idSb.toString());
		}
 
        // 生成推荐引擎  : 基于用户的协同过滤算法， 
        //还有基于物品的过滤算法，mahout 下面已经有很多实现
        //Recommender rec = new GenericUserBasedRecommender(model, nbh, sim);  
 
        Recommender rec = new GenericBooleanPrefUserBasedRecommender(dataModel, nbh, sim);
 
        // 为用户ID(1)推荐物品(数量2个)  
        List<RecommendedItem> recItemList = rec.recommend(1, 3);
 
        for (RecommendedItem item : recItemList) {
            System.out.println(item);
        }
	}
	
	/**
	 * 从内存中读取无偏好数据，计算相邻用户
	 * @throws TasteException
	 */
	public static void booleanUserCfBasedMemory() throws TasteException {
		//基于程序创建DataModel
		FastByIDMap<PreferenceArray> preferences = new FastByIDMap<>();
		//用户1喜欢物品1、2、3
		PreferenceArray userPref1 = new BooleanUserPreferenceArray(3);
		userPref1.set(0, new BooleanPreference(1, 101));
		userPref1.set(1, new BooleanPreference(1, 102));
		userPref1.set(2, new BooleanPreference(1, 103));
		// 用户2喜欢物品1、2、3、4 
		PreferenceArray userPref2 = new BooleanUserPreferenceArray(4);
		userPref2.set(0, new BooleanPreference(2, 101)); 
		userPref2.set(1, new BooleanPreference(2, 102)); 
		userPref2.set(2, new BooleanPreference(2, 103)); 
		userPref2.set(3, new BooleanPreference(2, 104)); 
		// 用户3喜欢物品1、4、5、7 
		PreferenceArray userPref3 = new BooleanUserPreferenceArray(4); 
		userPref3.set(0, new BooleanPreference(3, 101)); 
		userPref3.set(1, new BooleanPreference(3, 104)); 
		userPref3.set(2, new BooleanPreference(3, 105)); 
		userPref3.set(3, new BooleanPreference(3, 107)); 
		// 用户4喜欢物品1、3、4、6 
		PreferenceArray userPref4 = new BooleanUserPreferenceArray(4); 
		userPref4.set(0, new BooleanPreference(4, 101)); 
		userPref4.set(1, new BooleanPreference(4, 103));
		userPref4.set(2, new BooleanPreference(4, 104));
		userPref4.set(3, new BooleanPreference(4, 106)); 
		// 用户5喜欢物品1、2、3、4、5、6 
		PreferenceArray userPref5 = new BooleanUserPreferenceArray(6); 
		userPref5.set(0, new BooleanPreference(5, 101)); 
		userPref5.set(1, new BooleanPreference(5, 102)); 
		userPref5.set(2, new BooleanPreference(5, 103)); 
		userPref5.set(3, new BooleanPreference(5, 104)); 
		userPref5.set(4, new BooleanPreference(5, 105)); 
		userPref5.set(5, new BooleanPreference(5, 106)); 
		
		preferences.put(1, userPref1); 
		preferences.put(2, userPref2); 
		preferences.put(3, userPref3); 
		preferences.put(4, userPref4); 
		preferences.put(5, userPref5); 
		
		//构建Datamodel
		DataModel model = new GenericDataModel(preferences);
		//基于欧式距离计算用户相似度
		UserSimilarity similarity = new LogLikelihoodSimilarity(model);
		//查找最相邻的2个用户
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(4, similarity, model);
		for(int i=1; i<=5; i++) {
			long[] ids = neighborhood.getUserNeighborhood(i);
			StringBuilder idSb = new StringBuilder();
			for(long id : ids) {
				idSb.append(id + "(" + similarity.userSimilarity(i, id) + ")" + ",");
			}
			System.out.println("用户" + i + "的相邻用户：" + idSb.toString());
		}
		
	}
}
