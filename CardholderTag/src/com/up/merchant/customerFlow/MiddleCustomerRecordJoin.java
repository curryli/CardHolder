package com.up.merchant.customerFlow;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.up.util.Constant;
import com.up.util.MD5;
import com.up.util.MccTransfer;


public class MiddleCustomerRecordJoin extends Configured{

	/**
	 * 数据源：MiddleMerchantFilter.java
	 * 字段：商户品牌、卡号、MD5、消费次数、消费金额
	 * */
	public static class TransFilterMapper extends Mapper<Object, Text, Text, Text>{	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
				String[] tokens = value.toString().split(",");
				String cardId = tokens[1].trim();
				String md5 = tokens[2].trim();
				context.write(new Text(cardId), new Text("A"+value.toString()));
			}catch(Exception e){
				e.printStackTrace();
				context.getCounter("FirstTableMapper", "errorlog").increment(1); 
			}
		}
	}
	
	/**
	 * 数据源：DeriveForCustomerRecord
	 * 字段：
	 * 1.卡号
	 * 2.MD5
	 * 3.卡种
	 * 4.卡性质
	 * 5.卡品牌
	 * 6.卡等级
	 * 7.发卡机构
	 * bag:{受理机构、商户号、商户名、交易金额、交易时间}
	 * 
	 * */
	public static class SecondTableMapper extends Mapper<Object, Text, Text, Text>{	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			try{
				String[] tokens = value.toString().split(",");
				String cardId = tokens[0].trim();
				String md5 = tokens[1].trim();
				
				context.write(new Text(cardId), new Text("B"+value.toString()));
			}catch(Exception e){
				e.printStackTrace();
				context.getCounter("SecondTableMapper", "errorlog").increment(1); 
			}
		}
	}
	
	/**
	 * 
	 * 输出字段：
	 * 1.商户品牌
	 * 2.卡号
	 * 3.MD5
	 * 4.消费次数
	 * 5.消费金额
	 * 6.卡种
	 * 7.卡性质
	 * 8.卡品牌
	 * 9.卡等级
	 * 10.发卡机构
	 * 11.bag:{受理机构、商户号、商户名、交易金额、交易时间}
	 * 
	 * */
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		private static final Text EMPTY_TEXT = new Text("");  
		private ArrayList<String> listA = new ArrayList<String>();
		private ArrayList<String> listB = new ArrayList<String>();
		private String joinType = "inner";
		
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			//StringBuilder sb = new StringBuilder();
			listA.clear();
			listB.clear();
			for(Text line:lines){
				if(line.toString().charAt(0)=='A')
					listA.add(line.toString().substring(1));
				else if(line.toString().charAt(0)=='B'){                
					StringBuilder sb = new StringBuilder(); 					//去掉最前头的cardid和md5两个字段
					String str = line.toString().substring(1);
					String[] tokens = str.split(Constant.separator_1);
					for(int i=2; i<tokens.length; i++){
						if(i!=tokens.length-1)
							sb.append(tokens[i]+Constant.separator_1);
						else
							sb.append(tokens[i]);
					}
					listB.add(sb.toString());
				}	
			}
			
			 if (!listA.isEmpty() && !listB.isEmpty()) {  
                 for (String A : listA) {  
                     for (String B : listB) {             
                         context.write(new Text(A.trim()+Constant.separator_1+B), null);  
                     }  
                 }  
             }  
			
			//context.write(new Text(sb.toString()), new Text(""));
		}
	}

	public static boolean execute(String[] args) throws Exception{

		Configuration conf = new Configuration();
		conf.set("mapred.min.split.size", "1073741824");
		conf.set("mapred.job.queue.name", "queue3");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("cardClass", otherArgs[2]);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[2]),true);
		Job job = new Job(conf, "Middle Customer Record Join ");
		job.setJarByClass(MiddleCustomerRecordJoin.class);
		
		//job.setMapperClass(ConvertMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TransFilterMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, SecondTableMapper.class);
		job.setReducerClass(ConvertReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);                     //-----------------
		
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		return job.waitForCompletion(true);

	}
}
