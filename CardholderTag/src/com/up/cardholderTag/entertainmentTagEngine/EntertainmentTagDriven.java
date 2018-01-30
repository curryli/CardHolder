package com.up.cardholderTag.entertainmentTagEngine;
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
import java.util.Iterator;
import java.util.Set;

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


public class EntertainmentTagDriven extends Configured{
	
	/**
	 * @options
	 * @param otherArgs[0]	cupsdataPath
	 * @param otherArgs[1]	output
	 * @param otherArgs[2]	cardBinPath
	 * @param otherArgs[3]	startDate
	 * @param otherArgs[4]	endDate
	 * */
	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		conf.set("mapred.min.split.size", "1073741824");
		conf.set("mapred.job.queue.name", "queue3");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("inCardBinPath", otherArgs[2]);
		
		//DistributedCache.addCacheFile(new Path(cardbinPath).toUri(), conf); 
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[1]),true);
		Job job = new Job(conf, "Car Tag Feature");
		job.setJarByClass(EntertainmentTagDriven.class);
		
		job.setMapperClass(EntertainmentTagMapper.class);
		job.setReducerClass(EntertainmentTagReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);                     //-----------------
		
		//导入多个目标文件，从起始日期到结束日期
		String inputBase = otherArgs[0];			//   /user/hddtmn/tbl_common_his_trans_success
		String starDate = otherArgs[3];
		String endDate = otherArgs[4];
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
		}
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//return job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
