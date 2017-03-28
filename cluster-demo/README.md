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
