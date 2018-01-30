package com.up.merchant.customerFlow;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.up.util.Constant;
import com.up.util.MD5;
import com.up.util.MccTransfer;

/**
 * 计算酒店品牌的每一位客户的到店次数和消费金额
 * 
 * */
public class MiddleMerchantFilter extends Configured{
	
	public static class ConvertMapper extends Mapper<Object, Text, Text, Text>{
		private final static Calendar calendar = Calendar.getInstance();
		private Hashtable<String, String> merchantBrandTable = new Hashtable<String, String>();
		private String merchantMergeType = "default";
		
		//读入要进行连接的卡bin表
		@Override
		protected void setup(Context context) throws IOException{
			
			if(merchantMergeType.equals("upload")){
				Configuration conf = context.getConfiguration();
				FileSystem fs = FileSystem.get(conf);			

				//读取商户总分店信息表到distributedCache
				String merchantList = conf.get("merchantList");
				FSDataInputStream merchantListStream = fs.open(new Path(merchantList));
				BufferedReader reader = new BufferedReader(new InputStreamReader(merchantListStream));
				String line = null;	
				try{
					while((line = reader.readLine())!=null){
						String[] tokens = line.split(",");
						merchantBrandTable.put(tokens[0].trim(), tokens[1].trim());
					}
				}
				catch(Exception e){
					e.printStackTrace();
				}
				finally{
					reader.close();
				}
			}
		} 		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] tokens = value.toString().split(Constant.separator_1);
			StringBuilder sb = new StringBuilder();
			
			String cardId = tokens[0].trim();
			String md5 = tokens[1].trim();
			String acpt_ins = tokens[7].trim();						//受理机构代码
			String merchantBrand = null;
			if(merchantMergeType.equals("default")){
				String merchantName = tokens[13].trim();
				merchantBrand = Constant.getTargetBrand(merchantName);
				
			}
			else if(merchantMergeType.equals("upload")){
				String merchantName = tokens[13].trim();
				String merchantCD = tokens[12].trim();
				if((merchantBrand=merchantBrandTable.get(merchantCD))==null)
					merchantBrand = merchantName;
			}		
			
			//过滤掉非目标城市的酒店交易                                                                                      ++++选择地区++++
			if(!Constant.isTargetAcptIns(acpt_ins))
				return;
			context.write(new Text(merchantBrand+Constant.separator_1+cardId+Constant.separator_1+md5), value);
		}
	}
	
	/**
	 * output:
	 * 商户品牌、卡号、MD5、消费次数、消费金额
	 * */
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			
			StringBuilder sb = new StringBuilder();
			int times = 0;
			BigDecimal amount = new BigDecimal("0");			
			
			for(Text line: lines){
				times++;
				String[] tokens = line.toString().split(Constant.separator_1);
				amount = amount.add(new BigDecimal(tokens[15].trim()));
			}
			
			sb.append(key.toString()+Constant.separator_1);			//商户品牌，卡号，MD5
			sb.append(times+Constant.separator_1);					//消费次数
			sb.append(amount.toString());							//消费金额
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
		Job job = new Job(conf, "MiddleMerchantFilter");
		job.setJarByClass(MiddleMerchantFilter.class);
		
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
