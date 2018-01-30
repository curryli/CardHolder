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


public class CarTagListJoin extends Configured{

	/**
	 * 数据源：商旅概要标签表
	 * 输出字段：卡号，存在标志位，标签位1，标签位2，标签位3，标签位4，标签位5，标签位6，标签位7
	 * */
	public static class TransFilterMapper extends Mapper<Object, Text, Text, Text>{	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				String[] tokens = value.toString().split(Constant.separator_1);
				String cardNum = tokens[0].trim();
				int isExist = 0;
				for(int i=1; i<tokens.length; i++){
					if(!tokens[i].trim().equals("-1")){
						isExist = 1;
						break;
					}
				}
				String tagString = value.toString().substring(cardNum.length()+1, value.toString().length());
				
				context.write(new Text(cardNum), new Text("A"+isExist+Constant.separator_1+tagString));
			}catch(Exception e){
				e.printStackTrace();
				context.getCounter("SecondTableMapper", "errorlog").increment(1); 
			}
		}
	}
	
	/**
	 * 数据源：汽车概要标签表
	 * 输出字段：卡号，存在标志位，标签位1，标签位2，标签位3，标签位4，标签位5，标签位6，标签位7
	 * 
	 * */
	public static class SecondTableMapper extends Mapper<Object, Text, Text, Text>{	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				//String target = context.getConfiguration().get("cardClass");
				String[] new_car_set = {"新车上路","马路老手"};
				String[] volume_set = {"小排量车","中排量车","大排量车"};
				String[] fuelcard_set = {"油卡偏执狂"};
				String[] fuel_time_set = {"工作时段加油党","上班族"};
				String[] rent_set = {"租车达人"};
				String[] drive_degree_set = {"自驾发烧友"};
				String[] drive_scope_set = {"省内自驾","跨省自驾"};
				
				HashSet<String> workTimeSet = new HashSet<String>();
				workTimeSet.add("9-12");
				workTimeSet.add("13-17");
				
				StringBuilder sb = new StringBuilder();
				StringBuilder tagSb = new StringBuilder();
				String[] tokens = value.toString().split(",");
				
				String card_num = tokens[0].trim();
				//card_num = card_num.substring(2, card_num.length());
				int car_confindence = Integer.parseInt(tokens[5].trim());
				int is_new_car = Integer.parseInt(tokens[6].trim());						//是否近一年新车
				float low_refuel_times_pct = Float.parseFloat(tokens[11].trim());			//低排量加油次数占比
				float media_refuel_times_pct = Float.parseFloat(tokens[12].trim());		//中排量加油次数占比
				float high_refuel_times_pct = Float.parseFloat(tokens[13].trim());		//高排量加油次数占比
				int refuel_times = Integer.parseInt(tokens[8].trim());					//加油次数
				int fuel_card_times = Integer.parseInt(tokens[15].trim());				//买油卡次数
				String refuel_time_record = tokens[23].trim();							//各时段加油次数记录
				int rent_times = Integer.parseInt(tokens[55].trim());						//租车次数
				int drive_degree = Integer.parseInt(tokens[59].trim());					//自驾狂热度
				int drive_city_count = Integer.parseInt(tokens[61].trim());				//行车城市个数
				int drive_province_count = Integer.parseInt(tokens[62].trim());			//德国省份个数
				
				String new_car_tag = "-1";
				String volume_tag = "-1";
				String fuelcard_tag = "-1";
				String fuel_time_tag = "-1";
				String rent_tag = "-1";
				String drive_degree_tag = "-1";
				String drive_scope_tag = "-1";
				
				if(car_confindence == 2)				//自有汽车高置信度
				{
					if(is_new_car == 1)					//近一年新买车
						//new_car_tag = new_car_set[0];
						new_car_tag = "1";
					else
						//new_car_tag = new_car_set[1];
						new_car_tag = "2";
				}
				
				//汽车排量标签
				if(refuel_times>0){
					if(high_refuel_times_pct>=low_refuel_times_pct && high_refuel_times_pct>=media_refuel_times_pct)
						//volume_tag = volume_set[2];
						volume_tag = "3";
					if(media_refuel_times_pct>=low_refuel_times_pct && media_refuel_times_pct>=high_refuel_times_pct)
						//volume_tag = volume_set[1];
						volume_tag = "2";
					if(low_refuel_times_pct>=media_refuel_times_pct && low_refuel_times_pct>=high_refuel_times_pct)
						//volume_tag = volume_set[0];
						volume_tag = "1";
				}
				
				//油卡标签
				if(fuel_card_times>0)
					//fuelcard_tag = fuelcard_set[0];
					fuelcard_tag = "1";
				
				//加油时间段标签
				if(!refuel_time_record.equals(TagUtility.NULL)){
					String[] recordTokens = refuel_time_record.split(Constant.separator_2);
					int workTimeCount = 0;
					int totalCount = 0;
					for(String str: recordTokens){
						String[] strTokens = str.split(Constant.separator_3);
						totalCount += Integer.parseInt(strTokens[1].trim());
						if(workTimeSet.contains(strTokens[0].trim()))
							workTimeCount += Integer.parseInt(strTokens[1].trim());
					}
					if(totalCount>0){
						if((float)workTimeCount/(float)totalCount >= 0.5)
							//fuel_time_tag = fuel_time_set[0];
							fuel_time_tag = "1";
						else if((float)workTimeCount/(float)totalCount < 0.2)
							//fuel_time_tag = fuel_time_set[1];
							fuel_time_tag = "2";
					}
				}
				
				//租车标签
				if(rent_times>0)
					//rent_tag = rent_set[0];
					rent_tag = "1";
				
				if(drive_degree>=1){
					//自驾发烧友标签
					if(drive_degree>=2)
						//drive_degree_tag = drive_degree_set[0];
						drive_degree_tag = "1";
					
					//自驾范围
					if(drive_province_count>1)
						//drive_scope_tag = drive_scope_set[1];
						drive_scope_tag = "2";
					else{
						drive_scope_tag = "1";
					}
				}
				
				int tag = 1;
				if(new_car_tag.equals("-1") && volume_tag.equals("-1") && fuelcard_tag.equals("-1") && fuel_time_tag.equals("-1")
						&& rent_tag.equals("-1") && drive_degree_tag.equals("-1") && drive_scope_tag.equals("-1"))
					tag = 0;
				
				//sb.append(card_num+Constant.separator_1);
				sb.append(tag+Constant.separator_1);
				sb.append(new_car_tag+Constant.separator_1);
				sb.append(volume_tag+Constant.separator_1);
				sb.append(fuelcard_tag+Constant.separator_1);
				sb.append(fuel_time_tag+Constant.separator_1);
				sb.append(rent_tag+Constant.separator_1);
				sb.append(drive_degree_tag+Constant.separator_1);
				sb.append(drive_scope_tag);
				
				context.write(new Text(card_num), new Text("B"+sb.toString()));
			}catch(Exception e){
				e.printStackTrace();
				context.getCounter("FirstTableMapper", "errorlog").increment(1); 
			}
		}
	}
	
	/**
	 * 
	 * 输出字段：
	 * 1.卡号
	 * 2.商旅标签存在位
	 * 3.标签位1
	 * 4.标签位2
	 * 5.标签位3
	 * 6.标签位4
	 * 7.标签位5
	 * 8.标签位6
	 * 9.标签位7
	 * 10.汽车标签存在位
	 * 11.新车标签位
	 * 12.排量标签位
	 * 13.油卡标签位
	 * 14.加油时间标签位
	 * 15.租车标签位
	 * 16.自驾狂热度标签位
	 * 17.自驾范围标签位
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
			
			String defaultStr = "0,-1,-1,-1,-1,-1,-1,-1";
			StringBuilder sb = new StringBuilder();
			 if (!listA.isEmpty() && !listB.isEmpty()) {  
				 for(String strA:listA){
					 for(String strB:listB){
						 sb.append(key.toString() + Constant.separator_1);
		                 sb.append(strA + Constant.separator_1);
		                 sb.append(strB);
		                 context.write(new Text(sb.toString()), null);  
					 }
				 }
             } 
			 
			 if(!listA.isEmpty() && listB.isEmpty()){
				for(String strA:listA){
					sb.append(key.toString()+Constant.separator_1);
					sb.append(strA+Constant.separator_1);
					sb.append(defaultStr);
					context.write(new Text(sb.toString()), null); 
				}
			 }
			
			 if(listA.isEmpty() && !listB.isEmpty()){
				for(String strB:listB){
					sb.append(key.toString()+Constant.separator_1);
					sb.append(defaultStr+Constant.separator_1);
					sb.append(strB);
					context.write(new Text(sb.toString()), null); 
				}
			 }
		}
	}

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		conf.set("mapred.min.split.size", "1073741824");
		conf.set("mapred.job.queue.name", "queue3");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("cardClass", otherArgs[2]);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[2]),true);
		Job job = new Job(conf, "Cardholder Tag List Join.");
		job.setJarByClass(CarTagListJoin.class);
		
		//job.setMapperClass(ConvertMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TransFilterMapper.class);	//商旅
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, SecondTableMapper.class);	//汽车
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
