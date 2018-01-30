package com.up.cardholderTag.carTagEngine.join;
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


public class TagListJoinForAPP extends Configured{

	/**
	 * 数据源：商旅概要标签表
	 * 输出字段：卡号，存在标志位，标签位1，标签位2，标签位3，标签位4，标签位5，标签位6，标签位7
	 * */
	public static class TransFilterMapper extends Mapper<Object, Text, Text, Text>{	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				String[] tokens = value.toString().split(Constant.separator_1);
				StringBuilder sb =new StringBuilder();
				String cardNum = tokens[0].trim();
				
				if(tokens[1].equals("1"))
					sb.append("商旅人士"+Constant.separator_4);
				
//				if(tokens[3].equals("2"))
//					sb.append("空中飞人"+Constant.separator_4);
//				else if(tokens[3].equals("1"))
//					sb.append("偏好飞机"+Constant.separator_4);
				
				if(tokens[4].equals("2"))
					sb.append("旅游达人"+Constant.separator_4);
				else if(tokens[4].equals("1"))
					sb.append("喜欢旅游"+Constant.separator_4);
				
				if(tokens[5].equals("1"))
					sb.append("低端酒店"+Constant.separator_4);
				else if(tokens[5].equals("2"))
					sb.append("中端酒店"+Constant.separator_4);
				else if(tokens[5].equals("3"))
					sb.append("高端酒店"+Constant.separator_4);
//				
//				if(tokens[7].equals("1"))
//					sb.append("港澳台"+Constant.separator_4);
//				else if(tokens[7].equals("2"))
//					sb.append("东南亚"+Constant.separator_4);
//				else if(tokens[7].equals("3"))
//					sb.append("欧美出境"+Constant.separator_4);
//				
//				if(tokens[9].equals("1"))
//					sb.append("有车"+Constant.separator_4);
//				
//				if(tokens[10].equals("1"))
//					sb.append("新车上路"+Constant.separator_4);
//				else if(tokens[10].equals("2"))
//					sb.append("马路老手"+Constant.separator_4);
				
				if(tokens[11].equals("1") || tokens[11].equals("2"))
					sb.append("中小排量"+Constant.separator_4);
				else if(tokens[11].equals("2"))
					sb.append("大排量"+Constant.separator_4);
				
//				if(tokens[13].equals("2"))
//					sb.append("上班族"+Constant.separator_4);
//				
//				if(tokens[14].equals("1"))
//					sb.append("租车达人"+Constant.separator_4);
				
				if(tokens[15].equals("1"))
					sb.append("自驾发烧友"+Constant.separator_4);
				
				sb.setLength(sb.length()-Constant.separator_4.length());
				
				if(sb.length()<1)
					sb.append("未知");
				
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
				
				sb.append(tokens[1]+Constant.separator_1+tokens[2]+Constant.separator_1+tokens[3]+Constant.separator_1+tokens[4]);

				context.write(new Text(cardNum), new Text("B"+sb.toString()));
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
	 * 
	 * */
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		private static final Text EMPTY_TEXT = new Text("");  
		private ArrayList<String> listA = new ArrayList<String>();
		private ArrayList<String> listB = new ArrayList<String>();
		private String joinType = "inner";
		
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			listA.clear();
			listB.clear();
			for(Text line:lines){
				if(line.toString().charAt(0)=='A')
					listA.add(line.toString().substring(1));
				else if(line.toString().charAt(0)=='B'){                
					listB.add(line.toString().substring(1));
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
			
			String defaultStr = "未知,未知,未知,未知";
			String newKey = new StringBuilder().append(key.toString()).reverse().toString();		//卡号反转
			StringBuilder sb = new StringBuilder();
			 if (!listA.isEmpty() && !listB.isEmpty()) {  
				 for(String strA:listA){
					 for(String strB:listB){
						 sb.append(newKey + Constant.separator_1);
		                 sb.append(strB + Constant.separator_1);
		                 sb.append(strA);
		                 context.write(new Text(sb.toString()), null);  
					 }
				 }
             } 
			 
			 if(!listA.isEmpty() && listB.isEmpty()){
				for(String strA:listA){
					sb.append(newKey+Constant.separator_1);
					sb.append(defaultStr+Constant.separator_1);
					sb.append(strA);
					context.write(new Text(sb.toString()), null); 
				}
			 }
			
			 if(listA.isEmpty() && !listB.isEmpty()){
				for(String strB:listB){
					sb.append(newKey+Constant.separator_1);
					sb.append(strB+Constant.separator_1);
					sb.append("未知");
					context.write(new Text(sb.toString()), null); 
				}
			 }
		}
	}

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
		fs.delete(new Path(otherArgs[2]),true);
		Job job = new Job(conf, "Cardholder Tag Join For APP.");
		job.setJarByClass(TagListJoinForAPP.class);
		
		//job.setMapperClass(ConvertMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TransFilterMapper.class);	//概要标签
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, SecondTableMapper.class);	//性别标签
		job.setReducerClass(ConvertReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);                     //-----------------
		
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//return job.waitForCompletion(true);

	}
}
