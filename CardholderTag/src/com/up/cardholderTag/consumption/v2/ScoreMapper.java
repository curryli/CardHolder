package com.up.cardholderTag.consumption.v2;

/**
 * 从原始交易数据表里取
 * */

import java.io.IOException;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.up.util.Constant;
import com.up.util.MD5;
import com.up.util.TagUtility;

public class ScoreMapper extends Mapper<Object, Text, Text, Text>{
	
	//百分位分位点
	String percential = "0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,7.0,16.0,24.0,30.0,39.0,47.0,52.0,62.0,"
			+ "72.0,81.0,92.0,100.0,110.0,125.0,139.0,151.0,167.0,184.0,200.0,212.0,233.0,251.0,274.0,298.0,316.0,342.0,369.0,399.0,428.0,462.0,499.0,525.0,"
			+ "567.0,607.0,657.0,704.0,761.0,817.0,884.0,950.0,1000.0,1069.0,1152.0,1244.0,1340.0,1449.0,1555.0,1683.0,1819.0,1977.0,2096.0,"
			+ "2279.0,2482.0,2685.0,2931.0,3151.0,3460.0,3800.0,4150.0,4600.0,5000.0,5505.0,6125.0,6923.0,7832.0,8926.0,10000.0,11256.0,13219.0,"
			+ "15557.0,18998.0,22808.0,29558.0,40000.0,59209.0,113555.0,3.57137899E8";
	String END = "end";
	String priAcctNoConv = "-1";				//卡号
			
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
				
		//String target = context.getConfiguration().get("cardClass");
		String[] tokens = value.toString().split("\001");
		StringBuilder sb = new StringBuilder();
		
		String cardId = tokens[0].trim();
		String cs_trx_amt_l6m = tokens[1].trim();
		String month_count = tokens[2].trim();
		float avg_amt = Float.parseFloat(tokens[3].trim());
		
		int score = getScore(avg_amt);
		
		sb.append(cardId+Constant.separator_1);
		sb.append(cs_trx_amt_l6m+Constant.separator_1);
		sb.append(month_count+Constant.separator_1);
		sb.append(avg_amt+Constant.separator_1);
		sb.append(score+Constant.separator_1);
		sb.append(END);
		
		context.write(new Text(sb.toString()), new Text(""));
			
		
		
	}
	
	public int getScore(float amt){
		int score = 275;
		
		String[] rank = percential.split(",");
		
		for(int i = 0; i <rank.length; i++)
		{
			if(Double.parseDouble(rank[i])>amt)
				return 275+i*575/100;
		}
		
		return score;
	}

	//计算交易是否被冲正， trans_st = 3 交易未冲正
	public int getTrans_st(String cu_trans_st, String trans_id){
		int trans_st = 0;
		
		if(cu_trans_st == null || cu_trans_st.equals("") || cu_trans_st.length() < 5) {
			trans_st = 0;
		}
		else{
			if(cu_trans_st.equals("10000"))
				trans_st = 1;
			else if(cu_trans_st.substring(1,2).equals("1"))
				trans_st = 2;
			else if(cu_trans_st.substring(3,4).equals("1")){
				if(trans_id.equals("S33") || trans_id.equals("S32"))
					trans_st = 4;
				else
					trans_st = 3;
			}
			else 
				trans_st =5;
		}
		return trans_st;
		
	}
	
//	public static void main(String[] arg){
//		ScoreMapper app = new ScoreMapper();
//		System.out.println(app.getScore(60000));
//	}
}
