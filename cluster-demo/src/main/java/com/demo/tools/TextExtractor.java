package com.demo.tools;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
* @ClassName: TextExtractor 
* @Description: 正则匹配中文
* @author xuechen
* @date 2017年2月21日 下午5:05:40
*  
*/
public class TextExtractor {

	public static String getText(String doc) {
		
		//匹配中文字符
		String regex =  "([\u4e00-\u9fa5]+)";
		StringBuilder sb = new StringBuilder();
		
		Matcher matcher = Pattern.compile(regex).matcher(doc);
		while(matcher.find()) {
			sb.append(matcher.group());
		}
		
		return sb.toString();
	}
}
