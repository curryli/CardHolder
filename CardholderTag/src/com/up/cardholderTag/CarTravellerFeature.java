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


public class CarTravellerFeature extends Configured{
	
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
				//if(!Constant.isTargetAcptIns(temp))						//只筛选出目标受理地区
					//return;
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
				
				//17.交易类型
				String trans_id = tokens[31].replaceAll("\"", "");
				sb.append(trans_id+",");
				
				//18.mcc名称
				String mcc_name = Constant.getMccName(mcc);
				sb.append(mcc_name+",");
				
				//19.mcc标签类型
				String mcc_type = Constant.getMccType(mcc);
				sb.append(mcc_type);
				
				if(Constant.isCarTravellerMCC(mcc))
					context.write(new Text(cardId), new Text(sb.toString()));
			}
		}
	}
	
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		
		class TransItem{
			
			private String cardNum;
			private String md5;
			private String mcc;
			private String mccName;
			private String mchntName;
			private String time;
			private float trans_at;
			private String acptIns;
			private String city;
			private String province;
			private String realProvince;
			
			public TransItem(String cardNum, String md5, String mcc, String mccName,
					String mchntName, String time, float trans_at, String acptIns) {
				super();
				this.cardNum = cardNum;
				this.md5 = md5;
				this.mcc = mcc;
				this.mccName = mccName;
				this.mchntName = mchntName;
				this.time = time;
				this.trans_at = trans_at/100;
				this.acptIns = acptIns;
			}
			
			public TransItem(String line){
				String[] tokens = line.split(",");
				this.cardNum = tokens[0].trim();
				this.md5 = tokens[1].trim();
				this.mcc = tokens[11].trim();
				this.mccName = tokens[18].trim();
				this.mchntName = tokens[13].trim();
				this.time = tokens[8].trim().substring(0,14);
				this.trans_at = Float.parseFloat(tokens[15].trim())/100;
				this.acptIns = tokens[7].trim();
				this.city = Constant.getCity(this.acptIns);
				this.province = Constant.getProvince(this.city);
				this.realProvince = Constant.getCityKey(this.mchntName);
				
				//修正异地收单商户的所在地
				if(!realProvince.equals("null")){
					if(!realProvince.equals(province))
						this.province = this.realProvince;
				}
			}

			public String getCardNum() {
				return cardNum;
			}

			public void setCardNum(String cardNum) {
				this.cardNum = cardNum;
			}

			public String getMd5() {
				return md5;
			}

			public void setMd5(String md5) {
				this.md5 = md5;
			}

			public String getMcc() {
				return mcc;
			}

			public void setMcc(String mcc) {
				this.mcc = mcc;
			}

			public String getMccName() {
				return mccName;
			}

			public void setMccName(String mccName) {
				this.mccName = mccName;
			}

			public String getMchntName() {
				return mchntName;
			}

			public void setMchntName(String mchntName) {
				this.mchntName = mchntName;
			}

			public String getTime() {
				return time;
			}

			public void setTime(String time) {
				this.time = time;
			}

			public float getTrans_at() {
				return trans_at;
			}

			public void setTrans_at(float trans_at) {
				this.trans_at = trans_at;
			}
			
			public String getAcptIns() {
				return acptIns;
			}

			public void setAcptIns(String acptIns) {
				this.acptIns = acptIns;
			}
			
			public void add(TransItem item){
				this.trans_at += item.trans_at;
			}
			
			public String getCity() {
				return city;
			}

			public void setCity(String city) {
				this.city = city;
			}

			public String getProvince() {
				return province;
			}

			public void setProvince(String province) {
				this.province = province;
			}

			public String getRealProvince() {
				return realProvince;
			}

			public void setRealProvince(String realProvince) {
				this.realProvince = realProvince;
			}

			//比较两个时间差
			public long getTimeDistance(TransItem item) throws Exception{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
				Date date1 = sdf.parse(this.getTime());
				Date date2 = sdf.parse(item.getTime());
				return Math.abs((date1.getTime()-date2.getTime()))/(1000*60);
			}
			
			//判断两条记录是否属于同一笔交易
			public boolean isSameTrans(TransItem item) throws Exception{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
				if(this.mchntName.equals(item.mchntName)){
					if(this.getTimeDistance(item)<60)
						return true;
				}
				return false;
			}

			public String toString(){
				StringBuilder sb = new StringBuilder();
				sb.append(this.time+Constant.separator_3);
				sb.append(this.mcc+Constant.separator_3);
				sb.append(this.mchntName+Constant.separator_3);
				sb.append(this.acptIns+Constant.separator_3);
				sb.append(this.trans_at);
				return sb.toString();
			}
		}
		
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			int count = 0;
			StringBuilder sb = new StringBuilder();
			ArrayList<TransItem> list = new ArrayList<TransItem>();
			for(Text line: lines){
				count++;
			
				TransItem item = new TransItem(line.toString());
				list.add(item);
			}
			
			//按时间从先到后排序
			Comparator<TransItem> comparator = new Comparator<TransItem>(){
				
				@Override
				public int compare(TransItem o1, TransItem o2) {
					// TODO Auto-generated method stub
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
					Date date1;
					int flag = 1;
					try {
						date1 = sdf.parse(o1.getTime());
						Date date2 = sdf.parse(o2.getTime());

						if(date2.getTime()>date1.getTime())
							flag=-1;
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return flag;
				}
				
			};
			Collections.sort(list, comparator);			
			
			//合并属于同一个事务交易的记录项
			try{
				for(int i=list.size()-1; i>0; i--){
					if(list.get(i-1).isSameTrans(list.get(i)))
					{
						list.get(i-1).add(list.get(i));
						list.remove(list.get(i));
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			
//
//			sb.append(key.toString()+",");
//			for(int i=0; i<list.size(); i++){
//				if(i!=list.size()-1)
//					sb.append(list.get(i).toString()+Constant.separator_2);
//				else
//					sb.append(list.get(i).toString());
//					
//			}
			
			
			sb.append(key.toString()+",");
			sb.append(getCarAttribute(list)+",");
			sb.append(getMainDriveArea(list)+",");
			sb.append(getDriveLikeDegree(list));
			
			context.write(new Text(sb.toString()), new Text(""));
		}
		
		/**
		 * 计算该卡汽车的相关属性（私家车客货车区分、排量区分、是否使用油卡区分、是否可能套现欺诈）
		 * */
		public String getCarAttribute(ArrayList<TransItem> list){
			String carType="null";
			String cardDisplayment="null";
			String petrolCard = "0";
			String fraud = "0";
			StringBuilder sb = new StringBuilder();
			
			String category1 = "小排量车";
			String category2 = "中大排量车";
			String category3 = "客货车或油卡";
			String category4 = "油卡或套现";
			ArrayList<Float> cateList1 = new ArrayList<Float>();
			ArrayList<Float> cateList2 = new ArrayList<Float>();
			ArrayList<Float> cateList3 = new ArrayList<Float>();
			ArrayList<Float> cateList4 = new ArrayList<Float>();
			
			//遍历所有交易记录
			for(int i=0; i<list.size(); i++){
				if(list.get(i).getTrans_at()<=400){
					cateList1.add(list.get(i).getTrans_at());
				}
				else if(list.get(i).getTrans_at()>400 && list.get(i).getTrans_at()<=800){
					cateList2.add(list.get(i).getTrans_at());
				}
				else if(list.get(i).getTrans_at()>800 && list.get(i).getTrans_at()<=3000){
					cateList3.add(list.get(i).getTrans_at());
				}
				else if(list.get(i).getTrans_at()>3000){
					cateList4.add(list.get(i).getTrans_at());
				}
			}
			
			int count_private = cateList1.size()+cateList2.size();
			int count_public = cateList3.size()+cateList4.size();
			
			//判断私家车还是客货车
			//private-私家车
			//public-客货车
			if(count_private>=count_public)
				carType = "private";
			else
				carType = "public";
			
			//判断汽车排量. 1-小排量       2-中大排量
			if(carType == "public")
				cardDisplayment = "2";
			else{
				if(cateList1.size()>=cateList2.size())
					cardDisplayment = "1";
				else
					cardDisplayment = "2";
			}
			
			//判断汽车是否有使用加油卡的习惯
			for(int i=0; i<list.size(); i++){
				if(list.get(i).getTrans_at()%1000==0 || (list.get(i).getTrans_at()>1000 && list.get(i).getTrans_at()%500==0))
				{
					petrolCard = "1";
					break;
				}
			}
			
			//判断就否有套现可能
			if(cateList4.size()>0)
				fraud = "1";
			
			sb.append(carType+",");
			sb.append(cardDisplayment+",");
			sb.append(petrolCard+",");
			sb.append(fraud);
			return sb.toString();
		}
		
		/**
		 * 计算主要行驶城市
		 * 
		 * */
		public String getMainDriveArea(ArrayList<TransItem> list){
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for(int i = 0; i<list.size(); i++){
				if(map.get(list.get(i).getCity())!=null){
					map.put(list.get(i).getCity(), map.get(list.get(i).getCity())+1);
				}
				else
					map.put(list.get(i).getCity(), 1);
			}
			
			Iterator it = map.keySet().iterator();
			int max = 0;
			String mainArea = "null";
			while(it.hasNext()){
				String city = (String)it.next();
				if(map.get(city)>max){
					max = map.get(city);
					mainArea = city;
				}
			}
			return mainArea;
		}
		
		/**
		 * 计算自驾类型
		 * 
		 * @return 省外自驾、省内自驾、市内自驾
		 * */
		public String getDriveLikeDegree(ArrayList<TransItem> list){
			String innerCityDrive = "innerCity";
			String innerProvinceDrive = "innerProvince";
			String outerProvinceDrive = "outerProvinceDrive";
			Set citySet = new HashSet();
			Set provinceSet = new HashSet();
			
			for(int i=0; i<list.size(); i++){
				citySet.add(list.get(i).getCity());
				provinceSet.add(list.get(i).getProvince());
			}
			
			if(provinceSet.size()>1)
				return outerProvinceDrive;
			else if(citySet.size()>1)
				return innerProvinceDrive;
			else
				return innerCityDrive;
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
		Job job = new Job(conf, "Car Travel Feature");
		job.setJarByClass(CarTravellerFeature.class);
		
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
