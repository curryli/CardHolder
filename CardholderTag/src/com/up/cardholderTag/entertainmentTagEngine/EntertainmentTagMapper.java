package com.up.cardholderTag.entertainmentTagEngine;

import java.io.IOException;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.up.util.Constant;
import com.up.util.MD5;
import com.up.util.TagUtility;

public class EntertainmentTagMapper extends Mapper<Object, Text, Text, Text>{
	//private final static String cardbinPath = "hdfs://ha-dev-nn:8020/user/hddtmn/association_model/card_bin";
	private final static Calendar calendar = Calendar.getInstance();
	private Hashtable<String, String> joinData = new Hashtable<String, String>();
			
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
				
		//String target = context.getConfiguration().get("cardClass");
		String[] tokens = value.toString().split(",");
				
		if(tokens[22].replaceAll("\"", "").trim().equals(""))          //卡号为空
			return;
		else{
			for(int i = 0; i < tokens.length; i++)
				tokens[i] = tokens[i].replaceAll("\"", "").trim();
					
			//应答码00表示成功交易，剔除不成功交易
			String resp = tokens[25].replaceAll("\"", "").trim();
			if(!resp.equals("00"))
				return;
					
			StringBuilder sb = new StringBuilder();
			String temp = "";
					
			//0.卡号
			String cardId = tokens[22].replaceAll("\"", "").trim();
			sb.append(cardId+",");
					
			//1.MD5
			String md5 = MD5.GetMD5Code(cardId);
			sb.append(md5+",");
					
			//2.卡种
			temp = tokens[13].replaceAll("\"", "").trim();
			sb.append(temp+",");
					
			//3.卡性质
			temp = tokens[70].replaceAll("\"", "").trim();
			sb.append(temp+",");
					
			//4.卡品牌
			temp = tokens[71].replaceAll("\"", "").trim();
			sb.append(temp+",");
					
			//5.卡等级
			temp = tokens[73].replaceAll("\"", "").trim();
			sb.append(temp+",");
					
			//6.发卡机构代码
			String iss_ins_cd = tokens[19].replaceAll("\"", "").trim();
			sb.append(iss_ins_cd+",");
					
			//7.受理机构代码
			temp = tokens[21].replaceAll("\"", "").trim();
			//if(!Constant.isTargetAcptIns(temp))						//只筛选出目标受理地区
				//return;
			if(temp.length()<8)
				return;
			sb.append(temp+",");
					
					
			//8.时间
			String to_ts = tokens[63].replaceAll("\"| |:|\\.|-", "");
			//temp = to_ts.substring(0,14);            //yyyymmddHHMMSS
			temp = to_ts;
			sb.append(temp+",");
			if(temp.length()<10)
				return;
					
			//9.年
			String year = to_ts.substring(0,4);
			sb.append(year+",");
					
			//10.月份
			//String hour = to_ts.substring(8,2);
			String month = to_ts.substring(4, 6);
			sb.append(month+",");
					
			//11.商户类型mcc
			String mcc = tokens[48].replaceAll("\"", "").trim();
			sb.append(mcc+",");
					
			//12.商户号                                      ++++++++++++++++++++++++++++++++++++++++++++++++++
			temp = tokens[54].replaceAll("\"", "").trim();
			sb.append(temp+",");
					
			//13.商户名
			String merchantName = tokens[55].replaceAll("\"", "").trim();
			sb.append(merchantName+",");
						
			//14.终端号
			temp = tokens[52].replaceAll("\"", "").trim();
			sb.append(temp+",");
					
			//15.交易金额
			temp = tokens[39].replaceAll("\"", "");
			sb.append(temp+",");
					
			//16.卡bin
			String card_bin = tokens[43].replaceAll("\"", "");
			sb.append(card_bin+",");
					
			//17.交易类型
			String trans_id = tokens[31].replaceAll("\"", "");
			sb.append(trans_id+",");
					
			//18.mcc名称
			String mcc_name = Constant.getMccName(mcc);
			sb.append(mcc_name+",");
					
			//19.mcc标签类型
			String mcc_type = Constant.getMccType(mcc);
			sb.append(mcc_type);
					
			//key: 卡号+发卡机构代码
			if(TagUtility.ENTTM_MCC_SET.contains(mcc))
				context.write(new Text(cardId+Constant.separator_1+iss_ins_cd), new Text(sb.toString()));
			
		}
	}
}
