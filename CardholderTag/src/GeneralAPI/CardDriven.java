package GeneralAPI;

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

/*
* 打包注意事项
* 1.card
* 2.com.up.util
* 3.配置文件 conf_up
* 4.Driver中交易明细的文件路径   inputPath = inputBase+"/"+dateStr+"_correct;
* 5.修改reducer数   job.setNumReduceTasks(300); 
* */

public class CardDriven extends Configured{

	/**
	 * @options
	 * @param otherArgs[0]	cupsdataPath
	 * @param otherArgs[1]	output
	 * @param otherArgs[2]	startDate
	 * @param otherArgs[3]	endDate
	 * */
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf =new Configuration();
		conf.set("mapred.min.split.size", "1073741824");
		String mapreduceQueueName = "root.default";
		conf.set("mapreduce.job.queuename", mapreduceQueueName);
		
		conf.addResource("classpath:core-site.xml" );
		conf.addResource("classpath:hdfs-site.xml" );
		conf.addResource("classpath:mapred-site.xml" );
		conf.addResource("classpath:yarn-site.xml" );
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("inCardBinPath", otherArgs[2]);
		
		//DistributedCache.addCacheFile(new Path(cardbinPath).toUri(), conf); 
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[1]),true);
		Job job = new Job(conf, "card quality");
		job.setJarByClass(CardDriven.class);
		job.setMapperClass(CardMapper.class);
		job.setReducerClass(CardReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(300);                     //-----------------
		
		//导入多个目标文件，从起始日期到结束日期
		String inputBase = otherArgs[0];			//   /user/hddtmn/in_common_his_trans/
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
				String inputPath = inputBase+"/"+dateStr+"_correct";												
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
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
