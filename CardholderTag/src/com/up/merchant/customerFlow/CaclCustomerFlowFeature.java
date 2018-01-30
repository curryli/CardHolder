package com.up.merchant.customerFlow;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

/**
 * 计算商户的各档忠诚度客群特征、客户流向特征、品牌认可度
 * 修改于2014-07-28 17：05
 * 
 * @author wangjun
 * */
public class CaclCustomerFlowFeature extends Configured{
	
	public static class ConvertMapper extends Mapper<Object, Text, Text, Text>{
		
		class BrandInfo{
			private String brand;				//商户品牌
			private int times;					//消费次数
			private double amount;				//消费金额
			
			public BrandInfo(String brand, String amount){
				this.brand = brand;
				this.amount = Double.parseDouble(amount);
				this.times = 1;
			}

			public String getBrand() {
				return brand;
			}
			public void setBrand(String brand) {
				this.brand = brand;
			}

			public int getTimes() {
				return times;
			}

			public void setTimes(int times) {
				this.times = times;
			}
			
			public Double getAmount() {
				return amount;
			}

			public void setAmount(String amount) {
				this.amount = Double.parseDouble(amount);
			}

			public void addTimes(){
				this.times++;
			}
			
			public void addAmount(String amount){
				this.amount += Double.parseDouble(amount);
			}
			
			public String toString(){
				StringBuilder sb = new StringBuilder();
				sb.append(this.brand+Constant.separator_3);
				sb.append(this.times+Constant.separator_3);
				sb.append(this.amount);
				return sb.toString();
			}
		}
		
		/**
		 * 输出 value item:
		 * 0.商户品牌
		 * 1.卡号
		 * 2.MD5
		 * 3.消费次数
		 * 4.消费金额
		 * 5.卡信息
		 * 6.最常去商户消费记录
		 * 7.去过商户历史消费记录（按次数排序）
		 * 
		 * */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] tokens = value.toString().split(Constant.separator_1);
			String merchantBrand = tokens[0].trim();
			
			merchantBrand = Constant.getTargetBrand(merchantBrand);		// added in 2014.08.08  ********************************************
			if(!Constant.isTargetBrand(merchantBrand))					//+++++++++++++++++++临时性+++++++++++++++++
				return;
			
			StringBuilder cardInfoSb = new StringBuilder();
			String temp = null;
			temp = tokens[5].trim();					//卡种
			cardInfoSb.append(temp+Constant.separator_3);
			temp = tokens[6].trim();					//卡性质
			cardInfoSb.append(temp+Constant.separator_3);
			temp = tokens[7].trim();					//卡品牌	
			cardInfoSb.append(temp+Constant.separator_3);
			temp = tokens[8].trim();					//卡等级
			cardInfoSb.append(temp+Constant.separator_3);
			temp = tokens[9].trim();					//发卡机构
			cardInfoSb.append(temp);
			
			HashMap<String, BrandInfo> recordMap = new HashMap<String, BrandInfo>();
			String recordBag = tokens[10].trim();
			
			//只有一条酒店消费记录
			if(recordBag.indexOf(Constant.separator_2)==-1)
			{
				String[] reTokens = recordBag.split(Constant.separator_3);
				String brand = Constant.getTargetBrand(reTokens[2].trim());
				if((recordMap.get(brand)) != null){
					BrandInfo info = recordMap.get(brand);
					info.addTimes();
					info.addAmount(reTokens[3].trim());
				}
				else{
					BrandInfo info = new BrandInfo(brand, reTokens[3].trim());
					recordMap.put(brand, info);
				}
			}
			else{//有多条酒店消费记录
				String[] transRecords = recordBag.split(Constant.separator_2);		//每一条酒店消费记录
				for(String record: transRecords){
					String[] reTokens = record.split(Constant.separator_3);
					String brand = Constant.getTargetBrand(reTokens[2].trim());
					if((recordMap.get(brand)) != null){
						BrandInfo info = recordMap.get(brand);
						info.addTimes();
						info.addAmount(reTokens[3].trim());
					}
					else{
						BrandInfo info = new BrandInfo(brand, reTokens[3].trim());
						recordMap.put(brand, info);
					}
				}
			}
			
			ArrayList<BrandInfo> list = new ArrayList<BrandInfo>();
			Iterator it = recordMap.keySet().iterator();
			while(it.hasNext()){
				list.add(recordMap.get(it.next()));
			}
			Comparator<BrandInfo> comparator = new Comparator<BrandInfo>(){

				@Override
				public int compare(BrandInfo o1, BrandInfo o2) {
					// TODO Auto-generated method stub
					return o2.getTimes()-o1.getTimes();
				}
				
			};
			Collections.sort(list, comparator);
			
			String favoriteBrandString = getFavoriteBrandString(list);
			String sortedRecordString = getSortedRecordString(list);
			
			StringBuilder sb = new StringBuilder();
			sb.append(merchantBrand+Constant.separator_1);					//商户品牌
			sb.append(tokens[1].trim()+Constant.separator_1);				//卡号
			sb.append(tokens[2].trim()+Constant.separator_1);				//MD5
			sb.append(tokens[3].trim()+Constant.separator_1);				//消费次数
			sb.append(tokens[4].trim()+Constant.separator_1);				//消费金额
			sb.append(cardInfoSb.toString()+Constant.separator_1);			//卡信息
			sb.append(favoriteBrandString+Constant.separator_1);			//最常去商户消费记录
			sb.append(sortedRecordString);									//去过商户历史消费记录（按次数排序）
			
			context.write(new Text(merchantBrand), new Text(sb.toString()));	
		}
		
		//最常去商户消费记录
		private String getFavoriteBrandString(ArrayList<BrandInfo> list){
			StringBuilder sb = new StringBuilder();
			if(list.size()>0)
			{
				sb.append(list.get(0).getBrand()+Constant.separator_3);			//商户品牌名
				sb.append(list.get(0).getTimes()+Constant.separator_3);			//消费次数
				sb.append(list.get(0).getAmount());								//总消费金额
			}
			return sb.toString();
		}
		
		//去过商户历史消费记录（按次数排序）
		private String getSortedRecordString(ArrayList<BrandInfo> list){
			StringBuilder infoSb = new StringBuilder();
			for(int i = 0; i < list.size(); i++){
				infoSb.append(list.get(i).getBrand()+Constant.separator_3);		//商户品牌名
				infoSb.append(list.get(i).getTimes()+Constant.separator_3);		//消费次数
				infoSb.append(list.get(i).getAmount());							//总消费金额
				if(i != list.size()-1)
					infoSb.append(Constant.separator_2);
			}
			
			return infoSb.toString();
		}
	}
	
	public static class ConvertReducer extends Reducer<Text,Text,Text,Text>{
		
		class BrandInfo{
			private String brand;				//商户品牌
			private int times;					//消费次数
			private double amount;			//消费金额
			private int diff;				//在本品牌消费次数与目标品牌消费次数的差值
			
			public BrandInfo(String brand, String amount){
				this.brand = brand;
				this.amount = Double.parseDouble(amount);
				this.times = 1;
				this.diff = 0;
			}
			
			public BrandInfo(String brand, Double amount){
				this.brand = brand;
				this.amount = amount;
				this.times = 1;
				this.diff = 0;
			}
			
			public BrandInfo(String line){
				String[] tokens = line.split(Constant.separator_3);
				this.brand = tokens[0].trim();
				this.times = Integer.parseInt(tokens[1].trim());
				//this.amount = new BigDecimal(tokens[2].trim());
				this.amount = Double.parseDouble(tokens[2].trim());
				this.diff = 0;
			}

			public String getBrand() {
				return brand;
			}

			public void setBrand(String brand) {
				this.brand = brand;
			}

			public int getTimes() {
				return times;
			}

			public void setTimes(int times) {
				this.times = times;
			}
			
			public double getAmount() {
				return amount;
			}

			public void setAmount(String amount) {
				this.amount = Double.parseDouble(amount);
			}
			
			public void setAmount(double amount) {
				this.amount = amount;
			}

			public int getDiff() {
				return diff;
			}

			public void setDiff(int diff) {
				this.diff = diff;
			}

			public void addTimes(){
				this.times++;
			}
			
			public void addAmount(String amount){
				this.amount += Double.parseDouble(amount);
			}
			
			public String toString(){
				StringBuilder sb = new StringBuilder();
				sb.append(this.brand+Constant.separator_3);
				sb.append(this.times+Constant.separator_3);
				sb.append(this.amount);
				return sb.toString();
			}
		}
		
		class CustomerInfo{
			private String brand;							//商户品牌
			private String cardId;							//卡号
			private String md5;								//md5
			private int transTimes;							//在当前商户交易次数
			private double transAmout;						//在当前商户交易金额
			private String cardClass;						//卡种
			private String cardAttr;						//卡性质
			private String cardBrand;						//卡品牌
			private String cardLevel;						//卡等级
			private String insCd;							//发卡机构
			private BrandInfo favoriteBrand;				//最常去商户消费记录
			private ArrayList<BrandInfo> recordList;		//去过商户历史消费记录
			private int recordTimes;						//行业消费总次数
			
			public CustomerInfo(String line){
				String[] tokens = line.split(Constant.separator_1);
				this.brand = tokens[0].trim();
				this.cardId = tokens[1].trim();
				this.md5 = tokens[2].trim();
				this.transTimes = Integer.parseInt(tokens[3].trim());
				//this.transAmout = new BigDecimal(tokens[4].trim());
				this.transAmout = Double.parseDouble(tokens[4].trim());
				
				String[] cardInfoTokens = tokens[5].trim().split(Constant.separator_3);
				this.cardClass = cardInfoTokens[0].trim();
				this.cardAttr = cardInfoTokens[1].trim();
				this.cardBrand = cardInfoTokens[2].trim();
				this.cardLevel = cardInfoTokens[3].trim();
				this.insCd = cardInfoTokens[4].trim();
				
				this.favoriteBrand = new BrandInfo(tokens[6].trim());
				this.recordList = parseBrandList(tokens[7].trim());
				this.recordTimes = recordList.size();
			}

			public String getBrand() {
				return brand;
			}

			public void setBrand(String brand) {
				this.brand = brand;
			}

			public String getCardId() {
				return cardId;
			}

			public void setCardId(String cardId) {
				this.cardId = cardId;
			}

			public String getMd5() {
				return md5;
			}

			public void setMd5(String md5) {
				this.md5 = md5;
			}

			public int getTransTimes() {
				return transTimes;
			}

			public void setTransTimes(int transTimes) {
				this.transTimes = transTimes;
			}

			public double getTransAmout() {
				return transAmout;
			}

			public void setTransAmout(String transAmout) {
				this.transAmout = Double.parseDouble(transAmout);
			}

			public String getCardClass() {
				return cardClass;
			}

			public void setCardClass(String cardClass) {
				this.cardClass = cardClass;
			}

			public String getCardAttr() {
				return cardAttr;
			}

			public void setCardAttr(String cardAttr) {
				this.cardAttr = cardAttr;
			}

			public String getCardBrand() {
				return cardBrand;
			}

			public void setCardBrand(String cardBrand) {
				this.cardBrand = cardBrand;
			}

			public String getCardLevel() {
				return cardLevel;
			}

			public void setCardLevel(String cardLevel) {
				this.cardLevel = cardLevel;
			}

			public String getInsCd() {
				return insCd;
			}

			public void setInsCd(String insCd) {
				this.insCd = insCd;
			}

			public BrandInfo getFavoriteBrand() {
				return favoriteBrand;
			}

			public void setFavoriteBrand(BrandInfo favoriteBrand) {
				this.favoriteBrand = favoriteBrand;
			}

			public ArrayList<BrandInfo> getRecordList() {
				return recordList;
			}

			public void setRecordList(ArrayList<BrandInfo> recordList) {
				this.recordList = recordList;
			}		
			
			public int getRecordTimes() {
				return recordTimes;
			}

			public void setRecordTimes(int recordTimes) {
				this.recordTimes = recordTimes;
			}

			//解析历史消费记录
			private ArrayList<BrandInfo> parseBrandList(String line){
				ArrayList<BrandInfo> list = new ArrayList<BrandInfo>();
				String[] tokens = line.split(Constant.separator_2);
				for(int i = 0; i < tokens.length ; i++){
					BrandInfo info = new BrandInfo(tokens[i]);
					list.add(info);
				}
				return list;
			}
			
			//判断该客户的交易历史里是否包含目标品牌
			public boolean contain(String target){
				for(BrandInfo info:this.recordList){
					if(info.getBrand().equals(target))
						return true;
				}
				return false;
			}
			
			//获取对手品牌在该客户的交易情况
			public BrandInfo getRivalInfo(String rival){
				for(BrandInfo info:this.recordList){
					if(info.getBrand().equals(rival))
						return info;
				}
				return null;
			}
		}	
		
		/**
		 * 输出内容：
		 * 
		 * 0.商户品牌名
		 * bag1.到品牌消费过一次的客户的消费特征
		 * bag2.到品牌消费过多次的客户的消费特征和回访情况
		 * bag3.来过1次且同行业只消费1次客户群特征
		 * bag4.来过1次且同行业消费多次客户群特征与流向
		 * bag5.来过多次且在同行业为首选的客户群特征与流向
		 * bag6.来过多次且在同行业非首选的客户群特征与流向
		 * bag7.品牌认可度分析
		 * 
		 * */
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
			StringBuilder sb = new StringBuilder();
			
			ArrayList<CustomerInfo> allList = new ArrayList<CustomerInfo>();
			
			//只到店消费过1次且在同行业只消费过1次<忠诚度不明>
			ArrayList<CustomerInfo> onceFavorite = new ArrayList<CustomerInfo>();
			//只到店消费过1次且在同行业消费过多次<低忠诚度>
			ArrayList<CustomerInfo> onceNotFavorite = new ArrayList<CustomerInfo>();
			//到店消费过多次且在同行业只是首选<中等忠诚度>
			ArrayList<CustomerInfo> moreFavorite = new ArrayList<CustomerInfo>();
			//到店消费过多次但在同行业中不是首选<高忠诚度>
			ArrayList<CustomerInfo> moreNotFavorite = new ArrayList<CustomerInfo>();
			
			int totalCustomerCount = 0;
			int totalCustomerTimes = 0;
			double totalCustomerAmount = 0;
			
			for(Text line:lines){
				CustomerInfo info = new CustomerInfo(line.toString());
				//避免info在计算过程中某些值被修改使结果不准********************************************
				//CustomerInfo pureInfo  = new CustomerInfo(line.toString());            
				allList.add(info);
				totalCustomerCount++;
				totalCustomerTimes += info.getTransTimes();
				totalCustomerAmount += info.getTransAmout();
						
				if(info.getTransTimes() > 1)
				{
					if(info.getTransTimes() == info.getFavoriteBrand().getTimes()){
						moreFavorite.add(info);
					}
					else{
						moreNotFavorite.add(info);
					}
				}
				else if(info.getTransTimes()==1){
					if(info.getRecordTimes()==1)
						onceFavorite.add(info);
					else if(info.getRecordTimes()>1)
						onceNotFavorite.add(info);
					
				}
			}
			
			sb.append(key.toString() + Constant.separator_1);
			
			String temp = null;
			temp = this.getOnceOverallFeatureString(onceFavorite, onceNotFavorite);
			sb.append(temp + Constant.separator_1);
			temp = this.getMoreOverallFeatureString(moreFavorite, moreNotFavorite);
			sb.append(temp + Constant.separator_1);
			temp = this.getOnceFavoriteFeatureString(onceFavorite);
			sb.append(temp + Constant.separator_1);
			temp = this.getOnceNotFavoriteFeatureString(onceNotFavorite);
			sb.append(temp + Constant.separator_1);
			temp = this.getMoreFavoriteFeatureString(moreFavorite);
			sb.append(temp + Constant.separator_1);
			temp = this.getMoreNotFavoriteFeatureString(moreNotFavorite);
			sb.append(temp + Constant.separator_1);
			temp = this.getBrandRecognition(allList);
			sb.append(temp);
			
			context.write(new Text(sb.toString()), null);
		}
		
		//计算消费一次的客户的特征
		private String getOnceOverallFeatureString(ArrayList<CustomerInfo> list1, ArrayList<CustomerInfo> list2){
			if((list1.size()>0) || (list2.size()>0)){
				StringBuilder sb = new StringBuilder();
				int onceCustomerCount = 0;
				int onceCustomerTimes = 0;
				double onceCustomerAmount = 0;
				
				for(CustomerInfo info:list1){
					onceCustomerCount++;
					onceCustomerTimes += info.getTransTimes();
					onceCustomerAmount += info.getTransAmout();
				}
				
				for(CustomerInfo info:list2){
					onceCustomerCount++;
					onceCustomerTimes += info.getTransTimes();
					onceCustomerAmount += info.getTransAmout();
				}
				
				sb.append(onceCustomerCount + Constant.separator_3);
				sb.append(onceCustomerTimes + Constant.separator_3);
				sb.append(onceCustomerAmount);
				return sb.toString();
			}
			else 
				return Constant.EMPTY_TAG;
			
		}
		
		//计算消费多次客户特征，以及回访情况分布
		private String getMoreOverallFeatureString(ArrayList<CustomerInfo> list1, ArrayList<CustomerInfo> list2){
			if((list1.size()>0) || (list2.size()>0)){
				StringBuilder sb = new StringBuilder();
				int moreCustomerCount = 0;
				int moreCustomerTimes = 0;
				double moreCustomerAmount = 0;
				int times_2 = 0;
				int times_3 = 0;
				int times_4 = 0;
				int times_5 = 0;
				int times_6_10  = 0;
				int times_lt_10 = 0;
				
				for(CustomerInfo info:list1){
					moreCustomerCount++;
					moreCustomerTimes += info.getTransTimes();
					moreCustomerAmount += info.getTransAmout();
					
					if(info.getTransTimes()==2)
						times_2++;
					else if(info.getTransTimes()==3)
						times_3++;
					else if(info.getTransTimes()==4)
						times_4++;
					else if(info.getTransTimes()==5)
						times_5++;
					else if((info.getTransTimes()>5) && (info.getTransTimes()<=10))
						times_6_10++;
					else if(info.getTransTimes()>10)
						times_lt_10++;
				}
				for(CustomerInfo info:list2){
					moreCustomerCount++;
					moreCustomerTimes += info.getTransTimes();
					moreCustomerAmount += info.getTransAmout();
					
					if(info.getTransTimes()==2)
						times_2++;
					else if(info.getTransTimes()==3)
						times_3++;
					else if(info.getTransTimes()==4)
						times_4++;
					else if(info.getTransTimes()==5)
						times_5++;
					else if((info.getTransTimes()>5) && (info.getTransTimes()<=10))
						times_6_10++;
					else if(info.getTransTimes()>10)
						times_lt_10++;
				}
				
				sb.append(moreCustomerCount+Constant.separator_3);
				sb.append(moreCustomerTimes+Constant.separator_3);
				sb.append(moreCustomerAmount+Constant.separator_3);
				sb.append(times_2+Constant.separator_3);
				sb.append(times_3+Constant.separator_3);
				sb.append(times_4+Constant.separator_3);
				sb.append(times_5+Constant.separator_3);
				sb.append(times_6_10+Constant.separator_3);
				sb.append(times_lt_10);
				return sb.toString();
			}
			else
				return Constant.EMPTY_TAG;
			
		}
		
		/**
		 * 来过1次且同行业只消费1次客户群特征与流向
		 * 
		 * */
		private String getOnceFavoriteFeatureString(ArrayList<CustomerInfo> list){
			
			if(list.size()>0){
				StringBuilder sb = new StringBuilder();
				
				String basicFeature = getBasicFeatureString(list);
				String cardClassFeature = getCardClassFeatureString(list);
				String cardAttrFeature = getCardAttrFeatureString(list);
				String cardBrandFeature = getCardBrandFeatureString(list);
				String cardLevelFeature = getCardLevelFeatureString(list);
				String cardInsCdFeature = getInsCdFeatureString(list);
				
				sb.append(basicFeature + Constant.separator_2);
				sb.append(cardClassFeature + Constant.separator_2);
				sb.append(cardAttrFeature + Constant.separator_2);
				sb.append(cardBrandFeature + Constant.separator_2);
				sb.append(cardLevelFeature + Constant.separator_2);
				sb.append(cardInsCdFeature);
				return sb.toString();
			}
			else
				return Constant.EMPTY_TAG;
			
		}
		
		/**
		 * 来过1次且同行业消费多次客户群特征与流向
		 * 
		 * */
		private String getOnceNotFavoriteFeatureString(ArrayList<CustomerInfo> list){
			
			if(list.size()>0){
				StringBuilder sb = new StringBuilder();
				
				String basicFeature = getBasicFeatureString(list);
				String cardClassFeature = getCardClassFeatureString(list);
				String cardAttrFeature = getCardAttrFeatureString(list);
				String cardBrandFeature = getCardBrandFeatureString(list);
				String cardLevelFeature = getCardLevelFeatureString(list);
				String cardInsCdFeature = getInsCdFeatureString(list);
				String customerFlowFeature = getNotFavoriteCustomerFlowDirection(list);
				
				sb.append(basicFeature + Constant.separator_2);
				sb.append(cardClassFeature + Constant.separator_2);
				sb.append(cardAttrFeature + Constant.separator_2);
				sb.append(cardBrandFeature + Constant.separator_2);
				sb.append(cardLevelFeature + Constant.separator_2);
				sb.append(cardInsCdFeature + Constant.separator_2);
				sb.append(customerFlowFeature);
				
				return sb.toString();
			}
			else
				return Constant.EMPTY_TAG;
			
		}
		
		/**
		 * 来过多次且在同行业为首选的客户群特征与流向
		 * 
		 * */
		private String getMoreFavoriteFeatureString(ArrayList<CustomerInfo> list){
			
			if(list.size()>0){
				StringBuilder sb = new StringBuilder();
				
				String basicFeature = getBasicFeatureString(list);
				String cardClassFeature = getCardClassFeatureString(list);
				String cardAttrFeature = getCardAttrFeatureString(list);
				String cardBrandFeature = getCardBrandFeatureString(list);
				String cardLevelFeature = getCardLevelFeatureString(list);
				String cardInsCdFeature = getInsCdFeatureString(list);
				String customerFlowFeature = getFavoriteCustomerFlowDirection(list);
				
				sb.append(basicFeature + Constant.separator_2);
				sb.append(cardClassFeature + Constant.separator_2);
				sb.append(cardAttrFeature + Constant.separator_2);
				sb.append(cardBrandFeature + Constant.separator_2);
				sb.append(cardLevelFeature + Constant.separator_2);
				sb.append(cardInsCdFeature + Constant.separator_2);
				sb.append(customerFlowFeature);
				
				return sb.toString();
			}
			else
				return Constant.EMPTY_TAG;
			
		}
		
		/**
		 * 来过多次且在同行业非首选的客户群特征与流向
		 * 
		 * */
		private String getMoreNotFavoriteFeatureString(ArrayList<CustomerInfo> list){
			
			if(list.size()>0){
				StringBuilder sb = new StringBuilder();
				
				String basicFeature = getBasicFeatureString(list);
				String cardClassFeature = getCardClassFeatureString(list);
				String cardAttrFeature = getCardAttrFeatureString(list);
				String cardBrandFeature = getCardBrandFeatureString(list);
				String cardLevelFeature = getCardLevelFeatureString(list);
				String cardInsCdFeature = getInsCdFeatureString(list);
				String customerFlowFeature = getNotFavoriteCustomerFlowDirection(list);
				
				sb.append(basicFeature + Constant.separator_2);
				sb.append(cardClassFeature + Constant.separator_2);
				sb.append(cardAttrFeature + Constant.separator_2);
				sb.append(cardBrandFeature + Constant.separator_2);
				sb.append(cardLevelFeature + Constant.separator_2);
				sb.append(cardInsCdFeature + Constant.separator_2);
				sb.append(customerFlowFeature);
				
				return sb.toString();
			}
			else
				return Constant.EMPTY_TAG;
			
		}
		
		//统计每个list里的交易情况
		private String getBasicFeatureString(ArrayList<CustomerInfo> list){
			StringBuilder sb = new StringBuilder();
			int count = 0;
			int times = 0;
			double amount = 0;
			
			int industry_times = 0;
			double industry_amount = 0;
			
			for(CustomerInfo info : list){
				count ++;
				times += info.getTransTimes();
				amount += info.getTransAmout();	
				
				ArrayList<BrandInfo> brandList = info.getRecordList();
				for(BrandInfo brandInfo: brandList){
					if(!brandInfo.equals(info.getBrand())){
						industry_times += brandInfo.getTimes();
						industry_amount += brandInfo.getAmount();
					}
				}
			}
			
			sb.append(count + Constant.separator_3);
			sb.append(times + Constant.separator_3);
			sb.append(amount + Constant.separator_3);
			sb.append(industry_times + Constant.separator_3);
			sb.append(industry_amount);
			return sb.toString();
		}
		
		//计算每个list里的卡种情况
		private String getCardClassFeatureString(ArrayList<CustomerInfo> list){
			StringBuilder sb = new StringBuilder();
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			
			for(CustomerInfo info: list){
				if(map.get(info.getCardClass())!=null){
					int count = map.get(info.getCardClass());
					count++;
					map.put(info.getCardClass(), count);
				}
				else
					map.put(info.getCardClass(), 1);
			}

			Iterator it = map.keySet().iterator();
			while(it.hasNext()){
				String cardClass = (String)it.next();
				int count = map.get(cardClass);
				sb.append(cardClass + Constant.separator_4 + count + Constant.separator_3);
			}
			
			String output = sb.toString().substring(0,sb.toString().length()-Constant.separator_3.length());
			return output;
		}
		
		//计算每个list里的卡性质情况
		private String getCardAttrFeatureString(ArrayList<CustomerInfo> list){
			StringBuilder sb = new StringBuilder();
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			
			for(CustomerInfo info: list){
				if(map.get(info.getCardAttr())!=null){
					int count = map.get(info.getCardAttr());
					count++;
					map.put(info.getCardAttr(), count);
				}
				else
					map.put(info.getCardAttr(), 1);
			}

			Iterator it = map.keySet().iterator();
			while(it.hasNext()){
				String cardClass = (String)it.next();
				int count = map.get(cardClass);
				sb.append(cardClass + Constant.separator_4 + count + Constant.separator_3);
			}
			
			String output = sb.toString().substring(0,sb.toString().length()-Constant.separator_3.length());
			return output;
		}
		
		//计算每个list里的卡品牌情况
		private String getCardBrandFeatureString(ArrayList<CustomerInfo> list){
			StringBuilder sb = new StringBuilder();
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			
			for(CustomerInfo info: list){
				if(map.get(info.getCardBrand())!=null){
					int count = map.get(info.getCardBrand());
					count++;
					map.put(info.getCardBrand(), count);
				}
				else
					map.put(info.getCardBrand(), 1);
			}

			Iterator it = map.keySet().iterator();
			while(it.hasNext()){
				String cardClass = (String)it.next();
				int count = map.get(cardClass);
				sb.append(cardClass + Constant.separator_4 + count + Constant.separator_3);
			}
			
			String output = sb.toString().substring(0,sb.toString().length()-Constant.separator_3.length());
			return output;
		}
		
		//计算每个list里的卡等级情况
		private String getCardLevelFeatureString(ArrayList<CustomerInfo> list){
			StringBuilder sb = new StringBuilder();
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			
			for(CustomerInfo info: list){
				if(map.get(info.getCardLevel())!=null){
					int count = map.get(info.getCardLevel());
					count++;
					map.put(info.getCardLevel(), count);
				}
				else
					map.put(info.getCardLevel(), 1);
			}

			Iterator it = map.keySet().iterator();
			while(it.hasNext()){
				String cardClass = (String)it.next();
				int count = map.get(cardClass);
				sb.append(cardClass + Constant.separator_4 + count + Constant.separator_3);
			}
			
			String output = sb.toString().substring(0,sb.toString().length()-Constant.separator_3.length());
			return output;
		}
		
		//计算每个list里的发卡机构情况
		private String getInsCdFeatureString(ArrayList<CustomerInfo> list){
			StringBuilder sb = new StringBuilder();
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			
			for(CustomerInfo info: list){
				if(map.get(info.getInsCd())!=null){
					int count = map.get(info.getInsCd());
					count++;
					map.put(info.getInsCd(), count);
				}
				else
					map.put(info.getInsCd(), 1);
			}

			Iterator it = map.keySet().iterator();
			while(it.hasNext()){
				String cardClass = (String)it.next();
				int count = map.get(cardClass);
				sb.append(cardClass + Constant.separator_4 + count + Constant.separator_3);
			}
			
			String output = sb.toString().substring(0,sb.toString().length()-Constant.separator_3.length());
			return output;
		}
		
		/**
		 * 计算首选客户的其他同类商户流向
		 * 
		 * 对所有人
		 * 将去过的所有其他商户的次数进行各自累加，以总次数比较
		 * */
		private String getFavoriteCustomerFlowDirection(ArrayList<CustomerInfo> list){
			StringBuilder sb = new StringBuilder();
			String brand = list.get(0).getBrand();
			
			HashMap<String, BrandInfo> brandMap = new HashMap<String, BrandInfo>();
			for(CustomerInfo customerInfo: list){
				ArrayList<BrandInfo> brandList = customerInfo.getRecordList();
				for(BrandInfo brandInfo: brandList){
					if(brandMap.get(brandInfo.getBrand())!=null){
						BrandInfo temp = brandMap.get(brandInfo.getBrand());
						temp.setTimes(temp.getTimes()+1);
						//计算该客户到当前品牌的次数，比到目标品牌的次数多多少次
						temp.setDiff(temp.getDiff()+Math.abs(brandInfo.getTimes()-customerInfo.getTransTimes()));
						temp.setAmount(temp.getAmount()+brandInfo.getAmount());
					}
					else{
						BrandInfo newInfo = new BrandInfo(brandInfo.getBrand(), brandInfo.getAmount());
						newInfo.setTimes(1);
						//计算该客户到当前品牌的次数，比到目标品牌的次数多多少次
						newInfo.setDiff(Math.abs(brandInfo.getTimes()-customerInfo.getTransTimes()));
						brandMap.put(brandInfo.getBrand(), newInfo);
					}
				}
			}
			
			Iterator it = brandMap.keySet().iterator();
			ArrayList<BrandInfo> sortedList = new ArrayList<BrandInfo>();
			while(it.hasNext()){
				BrandInfo tempInfo = brandMap.get(it.next());
				if(!tempInfo.getBrand().equals(brand))				//把目标品牌剔除
					sortedList.add(tempInfo);
			}
			Comparator<BrandInfo> comparator = new Comparator<BrandInfo>(){

				@Override
				public int compare(BrandInfo o1, BrandInfo o2) {
					// TODO Auto-generated method stub
					return o2.getTimes()-o1.getTimes();
				}
				
			};
			Collections.sort(sortedList, comparator);				//对流向品牌的总消费次数进行排序
			
			//将排好序的品牌流向信息规范化输出
			if(sortedList.size()>0){
				for(int i = 0; i < sortedList.size() ; i ++){
					sb.append(sortedList.get(i).getBrand() + Constant.separator_4 + sortedList.get(i).getTimes() + Constant.separator_4 + sortedList.get(i).getDiff());
					if(i != sortedList.size()-1)
						sb.append(Constant.separator_3);
				}
				return sb.toString();
			}
			else
				return "null";
		}
		
		/**
		 * 计算非首选客户的其他同类商户流向
		 * 
		 * 对所有人，只选择商户列表里排名第一的商户
		 * 以商户的累计总消费次数进行比较
		 * 
		 * */
		private String getNotFavoriteCustomerFlowDirection(ArrayList<CustomerInfo> list){
			StringBuilder sb = new StringBuilder();
			String brand = list.get(0).getBrand();
			
			HashMap<String, BrandInfo> brandMap = new HashMap<String, BrandInfo>();
			for(CustomerInfo customerInfo: list){
				ArrayList<BrandInfo> brandList = customerInfo.getRecordList();
				for(BrandInfo brandInfo: brandList){
					//if((brandInfo.getTimes()==customerInfo.getFavoriteBrand().getTimes()) && !brandInfo.getBrand().equals(brand)){
					if((brandInfo.getTimes()==customerInfo.getFavoriteBrand().getTimes()) && brandInfo.getTimes()>customerInfo.getTransTimes()){
						if(brandMap.get(brandInfo.getBrand())!=null){
							BrandInfo temp = brandMap.get(brandInfo.getBrand());
							temp.setTimes(temp.getTimes()+1);
							//计算该客户到当前品牌的次数，比到目标品牌的次数多多少次
							temp.setDiff(temp.getDiff()+Math.abs(brandInfo.getTimes()-customerInfo.getTransTimes()));
							temp.setAmount(temp.getAmount()+brandInfo.getAmount());
						}
						else{
							BrandInfo newInfo = new BrandInfo(brandInfo.getBrand(), brandInfo.getAmount());
							newInfo.setTimes(1);
							//计算该客户到当前品牌的次数，比到目标品牌的次数多多少次
							newInfo.setDiff(Math.abs(brandInfo.getTimes()-customerInfo.getTransTimes()));
							brandMap.put(brandInfo.getBrand(), newInfo);
						}
					}
				}
			}
			
			Iterator it = brandMap.keySet().iterator();
			ArrayList<BrandInfo> sortedList = new ArrayList<BrandInfo>();
			while(it.hasNext()){
				BrandInfo tempInfo = brandMap.get(it.next());
				if(!tempInfo.getBrand().equals(brand))				//把目标品牌剔除
					sortedList.add(tempInfo);
			}
			Comparator<BrandInfo> comparator = new Comparator<BrandInfo>(){

				@Override
				public int compare(BrandInfo o1, BrandInfo o2) {
					// TODO Auto-generated method stub
					return o2.getTimes()-o1.getTimes();
				}
				
			};
			Collections.sort(sortedList, comparator);				//对流向品牌的总消费次数进行排序
			
			//将排好序的品牌流向信息规范化输出
			if(sortedList.size()>0){
				for(int i = 0; i < sortedList.size() ; i ++){
					sb.append(sortedList.get(i).getBrand() + Constant.separator_4 + sortedList.get(i).getTimes()+ Constant.separator_4 + sortedList.get(i).getDiff());
					if(i != sortedList.size()-1)
						sb.append(Constant.separator_3);
				}
				return sb.toString();
			}
			else
				return "null";	
		}
		
		/**
		 * 计算品牌认可度
		 * */
		private String getBrandRecognition(ArrayList<CustomerInfo> list){
			if(list.size()>0){
				String targetBrand = list.get(0).getBrand();
				int count = list.size();									//总人数
				HashMap<String, BrandInfo> brandMap = new HashMap<String, BrandInfo>();
				
				//找到与当前品牌客户交集最大的前十个对手品牌
				for(CustomerInfo customerInfo:list){
					ArrayList<BrandInfo> recordList = customerInfo.getRecordList();
					for(BrandInfo brand: recordList){
						//找到所有非目标品牌的对手品牌记录
						if(!brand.getBrand().equals(targetBrand)){
							if(brandMap.get(brand.getBrand())!=null){
								BrandInfo info = brandMap.get(brand.getBrand());
								info.setTimes(info.getTimes()+1);
							}
							else
							{
								BrandInfo rivaleBrand = new BrandInfo(brand.getBrand(), brand.getAmount());
								rivaleBrand.setTimes(1);								//品牌第一次出现
								brandMap.put(brand.getBrand(), rivaleBrand);
							}
						}
					}	
				}
				
				//对所有品牌按交集大小排序
				Iterator it = brandMap.keySet().iterator();
				ArrayList<BrandInfo> sortedList = new ArrayList<BrandInfo>();
				while(it.hasNext()){
					BrandInfo tempInfo = brandMap.get(it.next());
					sortedList.add(tempInfo);		
				}
				Comparator<BrandInfo> comparator = new Comparator<BrandInfo>(){
					@Override
					public int compare(BrandInfo o1, BrandInfo o2) {
						// TODO Auto-generated method stub
						return o2.getTimes()-o1.getTimes();
					}
				};
				Collections.sort(sortedList, comparator);				//对对手品牌的出现次数进行排序
				
				if(sortedList.size()==0)								//遇到奇葩情况，没有竞争对手，或者客户的忠诚度很高**********************
					return "null";
				
				//只取客户交集度前十的对手品牌进行分析
				StringBuilder sb = new StringBuilder();
				for(int i = 0; i < 10 && i<sortedList.size(); i++){
					BrandInfo rivalInfo = sortedList.get(i);
					int intersectionCount = rivalInfo.getTimes();		//交集人数
					int likeMeCount = 0;								//更认可本品牌人数
					int likeMeDiff = 0;									//认可本品牌的人来本店比去对方店多的次数
					int likeRivalCount = 0;								//更认可对手品牌人数
					int likeRivalDiff = 0;								//认可对手品牌的人到对方比来本店多的次数
					
					
					for(CustomerInfo customerInfo: list){
						if(customerInfo.contain(rivalInfo.getBrand())){
							BrandInfo rival = customerInfo.getRivalInfo(rivalInfo.getBrand());
							if(customerInfo.getTransTimes()>rival.getTimes()){
								likeMeCount++;
								likeMeDiff += customerInfo.getTransTimes()-rival.getTimes();
							}
							else if(customerInfo.getTransTimes()<rival.getTimes()){
								likeRivalCount++;
								likeRivalDiff += rival.getTimes()-customerInfo.getTransTimes();
							}
								
						}
					}
					
					sb.append(rivalInfo.getBrand()+Constant.separator_4);
					sb.append(intersectionCount+Constant.separator_4);
					sb.append(likeMeCount+Constant.separator_4);
					sb.append(likeMeDiff+Constant.separator_4);
					sb.append(likeRivalCount+Constant.separator_4);
					sb.append(likeRivalDiff);
					if(i!=9)
						sb.append(Constant.separator_3);
					
				}
				return sb.toString();
				
			}
			else
				return "null";
			
		}
	}
		

	public static boolean execute(String[] args) throws Exception{

		Configuration conf = new Configuration();
		//conf.set("mapred.min.split.size", "1073741824");
		//conf.set("mapred.job.queue.name", "queue3");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[1]),true);
		Job job = new Job(conf, "CaclCustomerFlowFeature");
		job.setJarByClass(CaclCustomerFlowFeature.class);
		
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
