package com.demo.tools;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** 
* @ClassName: CommonUtils 
* @Description: 通用工具类
* @author xuechen
* @date 2017年2月24日 下午2:57:42
*  
*/
public class CommonUtils {

	/**
	 * 从文件中获取文档总数
	 * @param path
	 * @return
	 * @throws IOException 
	 */
	@SuppressWarnings("deprecation")
	public static Long getCounts(String path) throws IOException {
		Long count = 0L;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		
		FSDataInputStream in = fs.open(new Path(path));
		count = Long.parseLong(in.readLine());
		in.close();
		
		return count;
	}
}
