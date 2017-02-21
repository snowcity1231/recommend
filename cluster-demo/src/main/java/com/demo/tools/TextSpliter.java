package com.demo.tools;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;


/** 
* @ClassName: TextSpliter 
* @Description: 使用IK Analyze分词
* @author xuechen
* @date 2017年2月21日 下午5:11:41
*  
*/
public class TextSpliter {
	
	public static List<String> getTerms(String str) throws IOException {
		
		List<String> terms = new ArrayList<>();
		StringReader reader = new StringReader(str);
		
		IKSegmenter ik = new IKSegmenter(reader, true);
		Lexeme lexeme = null;
		
		while((lexeme = ik.next()) != null) {
			terms.add(lexeme.getLexemeText());
		}
		
		return terms;
	}

}
