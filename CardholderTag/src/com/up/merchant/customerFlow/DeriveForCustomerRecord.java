package com.up.merchant.customerFlow;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.up.util.Constant;
import com.up.util.MD5;
import com.up.util.MccTransfer;

/**
 * 计算每一个持卡人在酒店业的所有消费记录
 * 
 * */
public class DeriveForCustomerRecord extends Configured{
	
	public static class ConvertMapper extends Mapper<Object, Text, Text, Text>{
		//private final static String cardbinPath = "hdfs://ha-dev-nn:8020/user/hddtmn/association_model/card_bin";
		private final static Calendar calendar = Calendar.getInstance();
		private Hashtable<String, String> joinData = new Hashtable<String, String>();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] tokens = value.toString().split(Constant.separator_1);
			StringBuilder sb = new StringBuilder();
			
			sb.append(tokens[0]+Constant.separator_1);      //卡号
			sb.append(tokens[1]+Constant.separator_1);      //MD5
			sb.append(tokens[2]+Constant.separator_1);      //卡种
			sb.append(tokens[3]+Constant.separator_1);      //卡性质
			sb.append(tokens[4]+Constant.separator_1);      //卡品牌
			sb.append(tokens[5]+Constant.separator_1);      //卡等级
			sb.append(tokens[6]);      //发卡机构
			
			context.write(new Text(sb.toString()), value);
		}
	}
	
	/**
	 * 输出字段说明：
	 * 
	 * 卡号
	 * MD5
	 * 卡种
	 * 卡性质
	 * 卡品牌
	 * 卡等级
	 * 发卡机构
	 * {酒店消费记录}
	 * 
	 * 酒店消费记录说明：
	 * 受理机构
	 * 商户号
	 * 商户名
	 * 交易金额
	 * 交易时间
	 * */
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			
			StringBuilder sb = new StringBuilder();
			StringBuilder recordsSb = new StringBuilder();
			for(Text line: lines){
				String[] tokens = line.toString().split(Constant.separator_1);
				String temp = "";
				
				temp = tokens[7].trim();				//受理机构
				recordsSb.append(temp+Constant.separator_3);
				temp = tokens[12].trim();				//商户号
				recordsSb.append(temp+Constant.separator_3);
				temp = tokens[13].trim();				//商户名
				recordsSb.append(temp+Constant.separator_3);
				temp = tokens[15].trim();				//交易金额
				recordsSb.append(temp+Constant.separator_3);
				temp = tokens[8].trim();				//交易时间
				
				recordsSb.append(temp+Constant.separator_2);
				
			}
			sb.append(key.toString()+Constant.separator_1+recordsSb.toString().substring(0,recordsSb.toString().length()-2));
			context.write(new Text(sb.toString()), new Text(""));
		}
		
	}

	public static boolean execute(String[] args) throws Exception{

		Configuration conf = new Configuration();
		conf.set("mapred.min.split.size", "1073741824");
		conf.set("mapred.job.queue.name", "queue3");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("cardClass", otherArgs[2]);
		
		//DistributedCache.addCacheFile(new Path(cardbinPath).toUri(), conf); 
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[1]),true);
		Job job = new Job(conf, "Derive for customer records. Input from : "+ otherArgs[0]);
		job.setJarByClass(DeriveForCustomerRecord.class);
		
		job.setMapperClass(ConvertMapper.class);
		job.setReducerClass(ConvertReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);                     //-----------------
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job.waitForCompletion(true);

	}
}
