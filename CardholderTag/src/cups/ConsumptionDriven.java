package cups;
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

/**
 * 测试环境打包注意事项
 * 1.consumption
 * 2.consumption.cardbin
 * 3.com.up.util
 * 4.配置文件 conf_yjy
 * 5.Driver中交易明细的文件路径   inputPath = inputBase+"/"+dateStr;
 * 
 * 生产环境打包注意事项
 * 1.consumption
 * 2.consumption.cardbin
 * 3.com.up.util
 * 4.配置文件 conf_up
 * 5.Driver中交易明细的文件路径   inputPath = inputBase+"/"+dateStr+"_correct;
 * 6.修改reducer数   job.setNumReduceTasks(300); 
 * */
public class ConsumptionDriven extends Configured{
	
	/**
	 * @options
	 * @param otherArgs[0]	cupsdataPath
	 * @param otherArgs[1]	output
	 * @param otherArgs[2]	cardBinPath
	 * @param otherArgs[3]	inPlatinumCardPath
	 * @param otherArgs[4]	startDate
	 * @param otherArgs[5]	endDate
	 * */
	public static void main(String[] args) throws Exception{

		//conf.set("mapred.min.split.size", "4294967296");
		//conf.set("mapred.job.queue.name", "queue3");
		Configuration conf =new Configuration();
//		conf.set("mapred.min.split.size", "4294967296");
		conf.set("mapred.min.split.size", "1073741824");
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
		
//		String yarnApplicationClasspath = "/etc/hadoop/conf:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop-yarn/.//*:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop-mapreduce/lib/*:/opt/cloudera/parcels/CDH-5.2.1-1.cdh5.2.1.p0.36/lib/hadoop/libexec/../../hadoop-mapreduce/.//*"; 
//		String mapreduceFrameworkName = "yarn";
//		
//		String mapreduceJobhistoryAddress = "bB0103004:10020";                             //********
//		String mapreduceJobhistoryDoneDir = "/user/history/done";
//		String mapreduceJobhistoryIntermediateDoneDir = "/user/history/done_intermediate";
//		
//		conf.set("fs.defaultFS", "hdfs://bB0103002:8020");
//		conf.set("yarn.application.classpath", yarnApplicationClasspath);
//		conf.set("mapreduce.framework.name", mapreduceFrameworkName);
//		
//		conf.set("mapreduce.jobhistory.address", mapreduceJobhistoryAddress);
//		conf.set("mapreduce.jobhistory.done-dir", mapreduceJobhistoryDoneDir);
//		conf.set("mapreduce.jobhistory.intermediate-done-dir", mapreduceJobhistoryIntermediateDoneDir);
//		conf.set("mapreduce.app-submission.cross-platform", "true");
//		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//		conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
//		
//		//ResourceManager存在HA机制
//		conf.set("yarn.resourcemanager.ha.enabled", "true");
//		conf.set("yarn.resourcemanager.recovery.enabled", "true");
//		conf.set("yarn.resourcemanager.store.class", "org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore");
//		conf.set("yarn.resourcemanager.ha.rm-ids", "rm690,rm657");
//		conf.set("yarn.resourcemanager.zk-address", "bB0103003:2181,bB0103004:2181,bB0103002:2181");
//		conf.set("yarn.resourcemanager.address.rm690"," bB0103002:8032");
//		conf.set("yarn.resourcemanager.address.rm657"," bB0103003:8032");
//		conf.set("yarn.resourcemanager.scheduler.address.rm690"," bB0103002:8030");
//		conf.set("yarn.resourcemanager.scheduler.address.rm657"," bB0103003:8030");
//		
//		//NameNode也存在HA机制
//		conf.set("fs.defaultFS", "hdfs://nameservice1");
//		conf. set("dfs.nameservices", "nameservice1");
//		conf.set("dfs.ha.namenodes.nameservice1", "namenode445,namenode684");
//		conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//		conf.set("dfs.namenode.rpc-address.nameservice1.namenode445", "bB0103002:8020");
//		conf.set("dfs.namenode.rpc-address.nameservice1.namenode684", "bB0103003:8020");



		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("inCardBinPath", otherArgs[2]);
		
		//DistributedCache.addCacheFile(new Path(cardbinPath).toUri(), conf); 
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[1]),true);
		Job job = new Job(conf, "Consumption Tag");
		job.setJarByClass(ConsumptionDriven.class);
		
		job.setMapperClass(ConsumptionMapper.class);
		job.setReducerClass(ConsumptionReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(300);                     //-----------------
		
		//导入多个目标文件，从起始日期到结束日期
		String inputBase = otherArgs[0];			//   /user/hddtmn/tbl_common_his_trans_success
		String inPlatinumCardPath = otherArgs[3];
		String starDate = otherArgs[4];
		String endDate = otherArgs[5];
		ArrayList<String> inputList = new ArrayList<String>();
		try{
		
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			Calendar sdate = Calendar.getInstance();
			Calendar edate = Calendar.getInstance();
			sdate.setTime(format.parse(starDate));
			edate.setTime(format.parse(endDate));
			while(sdate.before(edate)){
				String dateStr = format.format(sdate.getTime());
				String inputPath = inputBase+"/"+dateStr+"_correct";					//生产环境交易明细文件路径
//				String inputPath = inputBase+"/"+dateStr;								//测试环境交易明细文件路径 
				inputList.add(inputPath);
				
				sdate.add(Calendar.DATE, 1);
				
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		for(String str: inputList){
			FileInputFormat.addInputPath(job, new Path(str));
		}
		//FileInputFormat.addInputPath(job, new Path(inPlatinumCardPath));
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//return job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
