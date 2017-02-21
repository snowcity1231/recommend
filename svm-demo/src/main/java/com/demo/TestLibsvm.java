package com.demo;

/** 
* @ClassName: TestLibsvm 
* @Description: TODO
* @author xuechen
* @date 2017年2月20日 下午4:28:46
*  
*/
public class TestLibsvm {
	
	public static void main(String[] args) throws Exception {
		//train_bc存放SVM训练数据，model_r存放通过训练数据训练出的模型
		String[] arg = {"train_bc", "model_r"};
		//test_bc存放测试数据，model_r存放训练后的模型，out_r存放生成结果
		String[] parg = {"test_bc", "model_r", "out_r"};
		
		System.out.println("..........SVM训练开始............");
		
		//创建一个训练对象
		svm_train t = new svm_train();
		//创建一个预测或者分类对象
		svm_predict p = new svm_predict();
		t.main(arg);
		p.main(parg);
		
	}

}
