package com.up.cardholderTag.carTagEngine.join.multi;
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
import java.util.HashSet;
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
import com.up.util.TagUtility;


public class TagListJoinFirst extends Configured{

	/**
	 * 数据源：商旅概要标签表
	 * 输出字段：卡号，商旅存在标志位，标签位1，标签位2，标签位3，标签位4，标签位5，标签位6，标签位7，汽车存在标志位，标签位1，标签位2，标签位3，标签位4，标签位5，标签位6，标签位7
	 * */
	public static class TransFilterMapper extends Mapper<Object, Text, Text, Text>{	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				String[] tokens = value.toString().split(Constant.separator_1);
				StringBuilder sb =new StringBuilder();
				String cardNum = tokens[0].trim();
				
				for(int i = 1; i<tokens.length; i++){
					sb.append(tokens[i]);
					sb.append(Constant.separator_1);
				}
				if(sb.length()>=Constant.separator_1.length())
					sb.setLength(sb.length()-Constant.separator_1.length());
				
				context.write(new Text(cardNum), new Text("A"+sb.toString()));
			}catch(Exception e){
				e.printStackTrace();
				context.getCounter("FirstTableMapper", "errorlog").increment(1); 
			}
		}
	}
	
	/**
	 * 数据源：性别概要标签表
	 * 输出字段：卡号，存在标志位，标签位1，标签位2，标签位3，标签位4，标签位5，标签位6，标签位7
	 * 
	 * */
	public static class SecondTableMapper extends Mapper<Object, Text, Text, Text>{	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				String[] tokens = value.toString().split(Constant.separator_1);
				StringBuilder sb =new StringBuilder();
				String cardNum = tokens[0].trim();
				
				for(int i = 1; i<tokens.length; i++){
					sb.append(tokens[i]);
					sb.append(Constant.separator_1);
				}
				if(sb.length()>=Constant.separator_1.length())
					sb.setLength(sb.length()-Constant.separator_1.length());
				
				context.write(new Text(cardNum), new Text("B"+sb.toString()));
			}catch(Exception e){
				e.printStackTrace();
				context.getCounter("SecondTableMapper", "errorlog").increment(1); 
			}
		}
	}
	
	/**
	 * 数据源：休闲娱乐标签表
	 * 输出字段：卡号，存在标志位，标签位1，标签位2，标签位3，标签位4，标签位5，标签位6，标签位7
	 * 
	 * */
	public static class ThirdTableMapper extends Mapper<Object, Text, Text, Text>{	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				String[] tokens = value.toString().split(Constant.separator_1);
				StringBuilder sb =new StringBuilder();
				String cardNum = tokens[0].trim();
				
				for(int i = 5; i<tokens.length-1; i++){
					sb.append(tokens[i]);
					sb.append(Constant.separator_1);
				}
				if(sb.length()>=Constant.separator_1.length())
					sb.setLength(sb.length()-Constant.separator_1.length());
				
				context.write(new Text(cardNum), new Text("C"+sb.toString()));
			}catch(Exception e){
				e.printStackTrace();
				context.getCounter("SecondTableMapper", "errorlog").increment(1); 
			}
		}
	}
	
	/**
	 * 
	 * 输出字段：
	 * 1.卡号  (卡号反转)
	 * 2.性别
	 * 3.婚姻
	 * 4.标签位
	 * 5.休闲娱乐
	 * 
	 * */
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		private static final Text EMPTY_TEXT = new Text("");  
		private ArrayList<String> listA = new ArrayList<String>();
		private ArrayList<String> listB = new ArrayList<String>();
		private ArrayList<String> listC = new ArrayList<String>();
		private String joinType = "inner";
		
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			listA.clear();
			listB.clear();
			listC.clear();
			for(Text line:lines){
				if(line.toString().charAt(0)=='A')
					listA.add(line.toString().substring(1));
				else if(line.toString().charAt(0)=='B'){                
					listB.add(line.toString().substring(1));
				}	
				else if(line.toString().charAt(0)=='C'){
					listC.add(line.toString().substring(1));
				}
			}
			
			 /*
			String defaultStr = "0,-1,-1,-1,-1,-1,-1,-1";
			StringBuilder sb = new StringBuilder();
			 if (!listA.isEmpty() && !listB.isEmpty()) {  
				 sb.append(key.toString() + Constant.separator_1);
                 sb.append(listA.get(0) + Constant.separator_1);
                 sb.append(listB.get(0));
                 context.write(new Text(sb.toString()), null);  
             } 
			 
			 if(!listA.isEmpty() && listB.isEmpty()){
				sb.append(key.toString()+Constant.separator_1);
				sb.append(listA.get(0)+Constant.separator_1);
				sb.append(defaultStr);
				context.write(new Text(sb.toString()), null); 
			 }
			
			 if(listA.isEmpty() && !listB.isEmpty()){
				sb.append(key.toString()+Constant.separator_1);
				sb.append(defaultStr+Constant.separator_1);
				sb.append(listB.get(0));
				context.write(new Text(sb.toString()), null); 
			 } 
			*/
			
			//婚姻、性别、住址区域、年龄、总的消费次数、 是否有孩子、爱美食、爱打扮、移动支付、互联网支付
			String defaultStrA = "0,-1,-1,-1,-1,-1,-1,-1,0,-1,-1,-1,-1,-1,-1,-1";
			String defaultStrB = "未婚,男性,未知,25-35,0,-1,-1,-1,-1,-1";
			String defaultStrC = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0";
			String newKey = new StringBuilder().append(key.toString()).reverse().toString();		//卡号反转
			StringBuilder sb = new StringBuilder();
			
			 //A,B,C不空
			 if (!listA.isEmpty() && !listB.isEmpty() && !listC.isEmpty()) {  
				 for(String strA:listA){
					 for(String strB:listB){
						 for(String strC:listC){
							 sb.append(newKey + Constant.separator_1);
			                 sb.append(strB + Constant.separator_1);
			                 sb.append(strA + Constant.separator_1);
			                 sb.append(strC);
			                 context.write(new Text(sb.toString()), null); 
						 }
					 }
				 }
             } 
			 
			 //A空，B,C不空
			 if(listA.isEmpty() && !listB.isEmpty() && !listC.isEmpty()){
				for(String strB:listB){
					for(String strC:listC)
					{
						sb.append(newKey + Constant.separator_1);
		                sb.append(strB + Constant.separator_1);
		                sb.append(defaultStrA + Constant.separator_1);
		                sb.append(strC);
						context.write(new Text(sb.toString()), null); 
					}
				}
			 }
			
			 //B空，A,C不空
			 if(!listA.isEmpty() && listB.isEmpty() && !listC.isEmpty()){
				for(String strA:listA){
					for(String strC:listC)
					{
						sb.append(newKey + Constant.separator_1);
		                sb.append(defaultStrB + Constant.separator_1);
		                sb.append(strA + Constant.separator_1);
		                sb.append(strC);
						context.write(new Text(sb.toString()), null); 
					}
				}
			 }
			 
			 //C空，A,B不空
			 if(!listA.isEmpty() && !listB.isEmpty() && listC.isEmpty()){
				for(String strB:listB){
					for(String strA:listA)
					{
						sb.append(newKey + Constant.separator_1);
		                sb.append(strB + Constant.separator_1);
		                sb.append(strA + Constant.separator_1);
		                sb.append(defaultStrC);
						context.write(new Text(sb.toString()), null); 
					}
				}
			 }
			 
			 //A,B空，C不空
			 if(listA.isEmpty() && listB.isEmpty() && !listC.isEmpty()){
				for(String strC:listC){
					sb.append(newKey + Constant.separator_1);
	                sb.append(defaultStrB + Constant.separator_1);
	                sb.append(defaultStrA + Constant.separator_1);
	                sb.append(strC);
					context.write(new Text(sb.toString()), null); 
				}
			 }
			 
			//A,C空，B不空
			 if(listA.isEmpty() && !listB.isEmpty() && listC.isEmpty()){
				for(String strB:listB){
					sb.append(newKey + Constant.separator_1);
	                sb.append(strB + Constant.separator_1);
	                sb.append(defaultStrA + Constant.separator_1);
	                sb.append(defaultStrC);
					context.write(new Text(sb.toString()), null); 
				}
			 }
			 
			//B,C空，A不空
			 if(!listA.isEmpty() && listB.isEmpty() && listC.isEmpty()){
				for(String strA:listA){
					sb.append(newKey + Constant.separator_1);
	                sb.append(defaultStrB + Constant.separator_1);
	                sb.append(strA + Constant.separator_1);
	                sb.append(defaultStrC);
					context.write(new Text(sb.toString()), null); 
				}
			 }
		}
	}

	/*
	public static void main(String[] args) throws Exception{

		//conf.set("mapred.min.split.size", "4294967296");
		//conf.set("mapred.job.queue.name", "queue3");
		Configuration conf =new Configuration();
//		conf.set("mapreduce.job.queuename", "default");
//		conf.set("mapred.job.queue.name", "default");	
		String mapreduceQueueName = "root.default";
		conf.set("mapreduce.job.queuename", mapreduceQueueName);
		
		conf.addResource("classpath:core-site.xml" );
		conf.addResource("classpath:hdfs-site.xml" );
		conf.addResource("classpath:mapred-site.xml" );
		conf.addResource("classpath:yarn-site.xml" );
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());		
		
		//conf.set("mapred.min.split.size", "1073741824");
		//conf.set("mapred.job.queue.name", "queue3");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("cardClass", otherArgs[2]);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[3]),true);
		Job job = new Job(conf, "Cardholder Tag Join For APP.");
		job.setJarByClass(TagListJoinFirst.class);
		
		//job.setMapperClass(ConvertMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TransFilterMapper.class);	//概要标签
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, SecondTableMapper.class);	//性别标签
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, ThirdTableMapper.class);	//性别标签
		job.setReducerClass(ConvertReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);                     //-----------------
		
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//return job.waitForCompletion(true);

	}*/
	
	public static boolean execute(String[] args) throws Exception{

		//conf.set("mapred.min.split.size", "4294967296");
		//conf.set("mapred.job.queue.name", "queue3");
		Configuration conf =new Configuration();
//		conf.set("mapreduce.job.queuename", "default");
//		conf.set("mapred.job.queue.name", "default");	
		String mapreduceQueueName = "root.default";
		conf.set("mapreduce.job.queuename", mapreduceQueueName);
		
		conf.addResource("classpath:core-site.xml" );
		conf.addResource("classpath:hdfs-site.xml" );
		conf.addResource("classpath:mapred-site.xml" );
		conf.addResource("classpath:yarn-site.xml" );
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());		
		
		//conf.set("mapred.min.split.size", "1073741824");
		//conf.set("mapred.job.queue.name", "queue3");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("cardClass", otherArgs[2]);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[3]),true);
		Job job = new Job(conf, "Cardholder Tag Join For APP.");
		job.setJarByClass(TagListJoinFirst.class);
		
		//job.setMapperClass(ConvertMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TransFilterMapper.class);	//概要标签
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, SecondTableMapper.class);	//性别标签
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, ThirdTableMapper.class);	//性别标签
		job.setReducerClass(ConvertReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);                     //-----------------
		
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		return job.waitForCompletion(true);

	}
}
