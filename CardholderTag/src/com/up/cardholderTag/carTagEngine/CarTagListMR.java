package com.up.cardholderTag.carTagEngine;
/**
 * 计算汽车类的七个概要标签
 * 
 * */
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
import java.util.HashMap;
import java.util.HashSet;
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
import com.up.util.TagUtility;


public class CarTagListMR extends Configured{
	
	public static class ConvertMapper extends Mapper<Object, Text, Text, Text>{
		private final static Calendar calendar = Calendar.getInstance();
		private Hashtable<String, String> joinData = new Hashtable<String, String>();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
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
			card_num = card_num.substring(2, card_num.length());
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
			
			/*
			if(!new_car_tag.equals(TagUtility.NULL))
				tagSb.append(new_car_tag+Constant.separator_1);
			if(!volume_tag.equals(TagUtility.NULL))
				tagSb.append(volume_tag+Constant.separator_1);
			if(!fuelcard_tag.equals(TagUtility.NULL))
				tagSb.append(fuelcard_tag+Constant.separator_1);
			if(!fuel_time_tag.equals(TagUtility.NULL))
				tagSb.append(fuel_time_tag+Constant.separator_1);
			if(!rent_tag.equals(TagUtility.NULL))
				tagSb.append(rent_tag+Constant.separator_1);
			if(!drive_degree_tag.equals(TagUtility.NULL))
				tagSb.append(drive_degree_tag+Constant.separator_1);
			if(!drive_scope_tag.equals(TagUtility.NULL))
				tagSb.append(drive_scope_tag+Constant.separator_1);
			
			if(tagSb.length()>0)
				tagSb.setLength(tagSb.length()-Constant.separator_1.length());
			else
				tagSb.append(TagUtility.NULL);
			*/
			
			sb.append(card_num+Constant.separator_1);
			//sb.append(tagSb.toString());
			sb.append(new_car_tag+Constant.separator_1);
			sb.append(volume_tag+Constant.separator_1);
			sb.append(fuelcard_tag+Constant.separator_1);
			sb.append(fuel_time_tag+Constant.separator_1);
			sb.append(rent_tag+Constant.separator_1);
			sb.append(drive_degree_tag+Constant.separator_1);
			sb.append(drive_scope_tag);
			
			context.write(new Text(sb.toString()), new Text(""));
		}
	}
	
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			
		}
		
	}

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		//conf.set("mapred.min.split.size", "1073741824");
		//conf.set("mapred.job.queue.name", "queue3");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//conf.set("cardClass", otherArgs[2]);
		
		//DistributedCache.addCacheFile(new Path(cardbinPath).toUri(), conf); 
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[1]),true);
		Job job = new Job(conf, "Car Tag List");
		job.setJarByClass(CarTagListMR.class);
		
		job.setMapperClass(ConvertMapper.class);
		job.setReducerClass(ConvertReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);                     //-----------------
		
		//导入多个目标文件，从起始日期到结束日期
		/*
		String inputBase = otherArgs[0];			//   /user/hddtmn/tbl_common_his_trans_success
		String starDate = otherArgs[2];
		String endDate = otherArgs[3];
		ArrayList<String> inputList = new ArrayList<String>();
		try{
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			Calendar sdate = Calendar.getInstance();
			Calendar edate = Calendar.getInstance();
			sdate.setTime(format.parse(starDate));
			edate.setTime(format.parse(endDate));
			while(sdate.before(edate)){
				String dateStr = format.format(sdate.getTime());
				String inputPath = inputBase+"/"+dateStr+"/";
				inputList.add(inputPath+"00010000");
				inputList.add(inputPath+"00010344");
				inputList.add(inputPath+"00011000");
				inputList.add(inputPath+"00011100");
				inputList.add(inputPath+"00011200");
				inputList.add(inputPath+"00011600");
				inputList.add(inputPath+"00011900");
				inputList.add(inputPath+"00012210");
				inputList.add(inputPath+"00012220");
				inputList.add(inputPath+"00012400");
				inputList.add(inputPath+"00012600");
				inputList.add(inputPath+"00012900");
				inputList.add(inputPath+"00013000");
				inputList.add(inputPath+"00013310");
				inputList.add(inputPath+"00013320");
				inputList.add(inputPath+"00013600");
				inputList.add(inputPath+"00013900");
				inputList.add(inputPath+"00013930");
				inputList.add(inputPath+"00014200");
				inputList.add(inputPath+"00014500");
				inputList.add(inputPath+"00014520");
				inputList.add(inputPath+"00014900");
				inputList.add(inputPath+"00015210");
				inputList.add(inputPath+"00015500");
				inputList.add(inputPath+"00015800");
				inputList.add(inputPath+"00015840");
				inputList.add(inputPath+"00016100");
				inputList.add(inputPath+"00016400");
				inputList.add(inputPath+"00016500");
				inputList.add(inputPath+"00016530");
				inputList.add(inputPath+"00017000");
				inputList.add(inputPath+"00017310");
				inputList.add(inputPath+"00017700");
				inputList.add(inputPath+"00017900");
				inputList.add(inputPath+"00018200");
				inputList.add(inputPath+"00018500");
				inputList.add(inputPath+"00018700");
				inputList.add(inputPath+"00018800");
				inputList.add(inputPath+"00019999");
				
				sdate.add(Calendar.DATE, 1);
				
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		for(String str: inputList){
			FileInputFormat.addInputPath(job, new Path(str));
		}*/
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//return job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
