package cardScore;
//hadoop jar consumptionScore.jar cardScore.CalPercent -Dmapreduce.job.queuename=root.default xrli/CardholderTag/consumption_out xrli/CardholderTag/percent
 
//reduce阶段内存报错，估计是avgList存不下海量的数据  ，得用分桶法来搞，这边简单点就是统计    金额 >1千万的 个数， 1千万~100万的个数， 100万~十万的个数。。。。。。 每个范围对应相同的key
//假设求百分之20 位数，首先统计出一共有多少个，然后计算百分之20 位数对应第几个（假设是第12345个），然后从下往上数，比如金额0~100范围一共10000个，继续往上找，金额100~1000一共3000个，大于2345个，那应该就在这个范围内，在这个范围内排序（这样内存就够了），然后偏移即可
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 

import com.up.util.Constant;
  

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class CalPercent {
	
	public static float percentile(ArrayList<Float> data,float p){  
	    int n = data.size();  
	    Comparator<Float> c = new Comparator<Float>() {  
            @Override  
            public int compare(Float o1, Float o2) {  
                if((float)o1<(float)o2)  
                    return 1;  
                else return -1;  
            }  
        };     
        
	    data.sort(c);
	    float px =  p*(n-1);  
	    int i = (int)java.lang.Math.floor(px);  
	    float g = px - i;  
	    if(g==0){  
	        return data.get(i);  
	    }else{  
	        return (1-g)*data.get(i)+g*data.get(i+1);  
	    }  
	}  
	
	public static class CalPercentMapper extends Mapper<Object, Text, Text, FloatWritable>{
 
	 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	 
			String[] tokens = value.toString().split(Constant.separator_1);
		 
			FloatWritable avg_amt = new FloatWritable(Float.parseFloat(tokens[7].trim())); // 近6月广义月均消费金额
			  
			context.write(new Text(""), avg_amt);
		}		
	}
 

    public static class CalPercentReducer extends Reducer<Text,FloatWritable,Text,Text> {
        private Text result = new Text();
    
	    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
	    	ArrayList<Float> avgList = new ArrayList<Float>();
            for (FloatWritable val : values) {
            	avgList.add(val.get());
            }
            String percent_info = "10: " + String.valueOf(percentile(avgList,0.1f)) + "20: " + String.valueOf(percentile(avgList,0.2f)) + "30: " + String.valueOf(percentile(avgList,0.3f)); 
            result.set(percent_info);
            context.write(result, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
    	  Configuration conf = new Configuration();
    	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	  if (otherArgs.length != 2) {
    	    System.err.println("Usage: CalPercent <in> <out>");
    	    System.exit(2);
    	  }
    	  
  		  FileSystem fs = FileSystem.get(conf);
  		  fs.delete(new Path(otherArgs[1]),true);
  		
    	  Job job = new Job(conf, "CalPercent");
    	 
    	  job.setJarByClass(CalPercent.class);
    	  job.setMapperClass(CalPercentMapper.class); 
    	  job.setReducerClass(CalPercentReducer.class);
    	  //当map和reduce输出是不一样类型的时候就需要通过job.setMapOutputKeyClass和job.setMapOutputValueClas来设置map阶段的输出。
    	  job.setOutputKeyClass(Text.class);
    	  job.setOutputValueClass(FloatWritable.class);
    	  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    	  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    	  System.exit(job.waitForCompletion(true) ? 0 : 1);
    	}

}


 
