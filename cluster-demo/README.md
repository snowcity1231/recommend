###利用hadoop平台，对中文文档进行聚类

参考http://blog.csdn.net/sysu_arui/article/details/8546999

#####一、统计单词tf-idf，生成文档向量
*job1:统计文档总数  
*job2:统计单词在文档中出现次数  
*job3:统计单词在单个文档中的频率tf  
*job4:统计单词在每篇文档中的TFIDF值  
*job5:根据TFIDF值筛选关键词  
*job6:生成文档向量  
*job7:生成ID集合  
*job8:生成初始中心文件  
*job9:初次聚类，生成第一次聚类中心  
*重复job9，直到聚类中心不再改变，或者达到遍历数上限  
*job10:遍历文本，找出距离最近的聚类中心，对文本进行聚类  

启动hadoop之后，使用hadoop运行主函数  
参数说明：  
inputData path: 输入数据路径（HDFS），文件必须是二进制格式的文件  
output path: 输出数据路径（HDFS）  
tmp path: 中间临时文件路径 (HDFS)，程序运行结束，后台会删除里面的所有东西  
cluster number: 聚类中心数  
maxIterations: 最大迭代次数  

hadoop jar cluster-demo.jar hdfs://hadoop1:9000/inputdata/ hdfs://hadoop1:9000/output/ hdfs://hadoop1:9000/tmp/ 3 50