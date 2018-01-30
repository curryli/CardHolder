package com.up.cardholderTag;
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


public class DeriveForHotelTrans extends Configured{
	
	public static class ConvertMapper extends Mapper<Object, Text, Text, Text>{
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
				temp = tokens[19].replaceAll("\"", "").trim();
				sb.append(temp+",");
				
				//7.受理机构代码
				temp = tokens[21].replaceAll("\"", "").trim();
				if(!Constant.isTargetAcptIns(temp))						//只筛选出目标受理地区
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
				
				//16.交易类型
				String trans_id = tokens[31].replaceAll("\"", "");
				
				if(Constant.isTargetMCC(mcc))
					context.write(new Text(mcc), new Text(sb.toString()));
			}
		}
	}
	
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			int count = 0;
			StringBuilder sb = new StringBuilder();
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for(Text line: lines){
				count++;
				String[] tokens = line.toString().split(",");
				String cardNum = tokens[0];
				if(map.get(cardNum)==null)
					map.put(cardNum, 1);
			}
			sb.append(key.toString()+Constant.separator_1);
			sb.append(Constant.getMccType(key.toString().trim())+Constant.separator_1);
			sb.append(Constant.getMccName(key.toString().trim())+Constant.separator_1);
			sb.append(map.keySet().size()+Constant.separator_1);
			sb.append(count);
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
		Job job = new Job(conf, "Derive for hotel trans");
		job.setJarByClass(DeriveForHotelTrans.class);
		
		job.setMapperClass(ConvertMapper.class);
		job.setReducerClass(ConvertReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);                     //-----------------
		
		//导入多个目标文件，从起始日期到结束日期
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
				inputList.add(inputPath+"00011000");
				inputList.add(inputPath+"00012900");
				//inputList.add(inputPath+"00015800");
				
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
		return job.waitForCompletion(true);

	}
}
