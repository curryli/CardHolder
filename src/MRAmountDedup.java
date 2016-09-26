import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MRAmountDedup {
    //map将输入中的value复制到输出数据的key上，并直接输出
    public static class Map extends Mapper<Object,Text,Text,Text>{
    	public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{
            if(value!=null && !value.equals("")){
        	String[] paraArray = value.toString().split("\\001");
            context.write(new Text(paraArray[1]), new Text(""));
            }
        }
    }
 
    //reduce将输入中的key复制到输出数据的key上，并直接输出
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        //实现reduce函数
    	private Text amountListText = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	context.write(key, new Text(""));
        }
    }
        
        
    public static void main(String[] args) throws Exception {
  	  Configuration conf = new Configuration();
  	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  	  //GenericOptionsParser这个类，它的作用是将命令行中参数自动设置到变量conf中。很轻松就可以将参数与代码分离开。


  	 Job job = new Job(conf, "Card amount list MR");
  	 
  	 job.setJarByClass(MRAmountDedup. class );
  	 job.setMapperClass(Map. class );
//    job.setCombinerClass(Reduce. class );
     job.setReducerClass(Reduce. class );
     job.setOutputKeyClass(Text. class );
     job.setOutputValueClass(Text. class );
  	  FileInputFormat.addInputPath(job, new Path("xrli/GetFromHive"));
  	  FileOutputFormat.setOutputPath(job, new Path("xrli/AmountDedup"));
  	  System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}


}



/***************************
006d0e650150f2f848f8bad8bdebf335100000306091417
006d14677bebea7a5b45d8cc9c08f063200000306194620
006d16ec0770aec0d0ec9f9b6c4fdd6b250000307215032
006d17d12b833c6e7faeafdf8dbafb65700000305162139
006d17fd832b037ec0fc0a9c4d1e84e810000000302115508
006d1971572f3ccf940c00749aab11ea1000000307150238
006d19ff1b6eedf0ae6c077df615989923000000309202336
006d262b21364054ffae7d068aaaf08c20000000303150332
006d3806e67f91b4761cf3494715215e10400000305104100

提取中间的并去重变成

10000
20000
25000
70000
1000000
100000
2300000
2000000
1040000

**************/
