package com.up.util;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;



public class FonctionTest {
	class BrandInfo{
		private String brand;				//商户品牌
		private int times;					//消费次数
		private double amount;				//消费金额
		
		public BrandInfo(String brand, String amount){
			this.brand = brand;
			this.amount = Double.parseDouble(amount);
			this.times = 0;
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

	class Info{
		String brand;
		int times;
		
		public Info(String brand){
			this.brand = brand;
			this.times = 0;
		}
		
		public Info(String brand, int times){
			this.brand = brand;
			this.times = times;
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
		
	}
	
	public void mapTest(){
		System.out.println("=============================");
		
		Info info = new Info("Gucci");
		HashMap<String, Info> map = new HashMap<String, Info>();
		map.put("Gucci", info);
		System.out.println(map.get("Gucci").getTimes());
		
		Info a = map.get("Gucci");
		a.setTimes(5);
		System.out.println(map.get("Gucci").getTimes());
		
		if(map.get("Amony")==null)
			map.put("Amony", new Info("Amony",12));
		
		
		System.out.println("=====result===========");
		System.out.println(map.get("Gucci").getTimes());
		
		
	}
	
	public void mapTest2(){
		System.out.println("=============================");
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		map.put("good", 1);
		int count = map.get("good");
		count++;
		map.put("good", count);
		System.out.println(map.get("good"));
	}

	public void splitTest(){
		String str = "七天连锁酒店,196212263602041152363,32fc5a91784859a3e6410bbfd8e94a08,4,50912.0,01,01,12,0,01020000,48125810@812440071710015@7天酒店广州环市中店@2114.0@20131216202150348114||48125810@812440071710015@7天酒店广州环市中店@6375.0@20131216202030972691||48125810@812440071710015@7天酒店广州环市中店@10999.0@20131216202057073157||48125810@812440071710015@7天酒店广州环市中店@31424.0@20131216202124783669";
		String bag = "48125810@812440071710015@7天酒店广州环市中店@2114.0@20131216202150348114||48125810@812440071710015@7天酒店广州环市中店@6375.0@20131216202030972691||48125810@812440071710015@7天酒店广州环市中店@10999.0@20131216202057073157}48125810@812440071710015@7天酒店广州环市中店@31424.0@20131216202124783669";
		String[] transRecords = bag.split("\\|\\|");
		System.out.println(")))))");
		System.out.println(transRecords[1]);
		//System.out.println(splitFunction(str));
	}
	
	public String splitFunction(String str){
		String[] tokens = str.split(Constant.separator_1);
		String merchantBrand = tokens[0].trim();
		
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
				System.out.println("+++++++++++++++++++++");
				System.out.println(record);
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
				return o1.getTimes()-o2.getTimes();
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
		
		return sb.toString();
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
	
			
	//输出从起始日期到结束日期之间的所有日期
	public void DateTest(String starDate, String endDate){
		String inputBase = "/user/hddtmn/tbl_common_his_trans_success";
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		try{
			Calendar sdate = Calendar.getInstance();
			Calendar edate = Calendar.getInstance();
			sdate.setTime(format.parse(starDate));
			edate.setTime(format.parse(endDate));
			while(sdate.before(edate)){
				String str = format.format(sdate.getTime());
				String path = inputBase+"/"+str+"/";
				System.out.println(path+"00011000");
				System.out.println(path+"00012900");
				System.out.println(path+"00015800");
				
				sdate.add(Calendar.DATE, 1);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	public static String cardNumTransfer(String str){
		String newStr = str;
		String sub = newStr.substring(5, newStr.length()-4);
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i<sub.length(); i++)
			sb.append("*");
		newStr = newStr.replace(sub, sb.toString());
		return newStr;
	}
	
	public static void testCalender(){
		String str = "20141201000522";
		String str2 = "20141204120522";
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		try{
			Date dt = sdf.parse(str);
			calendar.setTime(dt);
			System.out.println(TagUtility.WEEK_DAYS[calendar.get(calendar.DAY_OF_WEEK)-1]);
			System.out.println(calendar.MONDAY);
			Date dt2 = sdf.parse(str2);
			calendar.setTime(dt2);
			System.out.println(TagUtility.WEEK_DAYS[calendar.get(calendar.DAY_OF_WEEK)-1]);
			int time = Integer.parseInt(str.substring(8,10));
			System.out.println("时间段："+getTimeIntervelName(time));
		}
		catch(ParseException e){
			e.printStackTrace();
		}
	}
	
	public static String getTimeIntervelName(int str){	
		for(int i=TagUtility.TIME_INTERVAL.length-1; i >0 ; i--){
			if(str>=TagUtility.TIME_INTERVAL[i-1] && str<TagUtility.TIME_INTERVAL[i]){
				return TagUtility.TIME_INTERVAL_NAME[i];
			}
		}
		return TagUtility.TIME_INTERVAL_NAME[0];
	}
	
	//去掉stringbuilder的最后几个字符
	public static String deleSbString(StringBuilder sb){
		String st = "123";
		sb.append("tom");
		sb.append(":");
		sb.append(st);
		sb.setLength(sb.length()-st.length());
		//System.out.println(sb.toString());
		return st;
	}
	
	public static long getDayInterval(String time1, String time2){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		Date date1;
		Date date2;
		try {
			date1 = sdf.parse(time1);
			date2 = sdf.parse(time2);
			return Math.abs((date1.getTime()-date2.getTime()))/(1000*60*60*24);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	public static String  getStandardDevition(ArrayList<Double> list){
		DecimalFormat df = new DecimalFormat("######0.0000");
		double  sum = 0f;
		
		for(double  elem : list)
			sum += elem;
		double  mean = sum/list.size();				//均值
		
		sum = 0f;
		for(double elem : list)
		{
			sum += (elem-mean)*(elem-mean);
		}
		
		return df.format(Math.sqrt(sum/list.size()));
	}
			
	public static void main(String[] args) {
		if(Constant.isTargetAcptIns("00012911"))
			System.out.println("yesyesyes");
		else
			System.out.println("nononononono");
		System.out.println(Constant.getTargetBrand("北京七天酒店三元桥店sd"));
		String str = "15370247033802777";
		System.out.println(str.substring(4,8));
		BigDecimal b = new BigDecimal("0");
		b=b.add(new BigDecimal("23"));
		System.out.println(b.toString());
		
		System.out.println("=============================");
		new FonctionTest().DateTest("20140101", "20140331");;;
		
		System.out.println(System.getProperty("user.dir"));
		System.out.println(cardNumTransfer("6222280001520367"));
		String strTest = "146222280001520367";
		System.out.println(strTest.substring(2,4));
		testCalender();

		//去掉stringbuilder的最后几个字符
		StringBuilder sb = new StringBuilder();
		System.out.println(deleSbString(sb));
		System.out.println(sb.toString());
		
		System.out.println((int)getDayInterval("20141201120503", "20141201132304"));
		System.out.println(str.substring(2,str.length()));
		
		String testStr = "234234234,3,-1,4,1,2,3,1";
		String[] tokens = testStr.split(Constant.separator_1);
		testStr = testStr.substring(tokens[0].length()+1, testStr.length());
		System.out.println("final str: "+ testStr);
		

		ArrayList<Double> list = new ArrayList<Double>();
		list.add(23.4);
//		list.add(43.3);
//		list.add(35.6);
//		list.add(33.4);
		System.out.println(getStandardDevition(list));
		
		String strT = "20150810153623";
		System.out.println(strT.substring(8,10));
		
		ArrayList<String> monthList = new ArrayList<String>();
		HashSet set = new HashSet();
		monthList.add("201506");
		monthList.add("201504");
		monthList.add("201502");
		set.add("201506");
		set.add("201504");
		set.add("201502");
		for(int i = 0; i<monthList.size(); i++)
			System.out.println(monthList.get(i));
//		Comparator<String> comparator = new Comparator<String>(){
//
//			@Override
//			public int compare(String o1, String o2) {
//				// TODO Auto-generated method stub
//				return Integer.parseInt(o1)-Integer.parseInt(o2);
//			}
//			
//		};
		Collections.sort(monthList);
		for(int i = 0; i<monthList.size(); i++)
			System.out.println(monthList.get(i));
		
		StringBuilder monthSb = new StringBuilder();
		Iterator it = set.iterator();
		while(it.hasNext()){
			monthSb.append((String)it.next()+Constant.separator_2);
		}
		if(monthSb.length()>Constant.separator_2.length())
			monthSb.setLength(monthSb.length()-Constant.separator_2.length());
		System.out.println(monthSb.toString());		
		
		String newString = "abcdefg";
		System.out.println(new StringBuilder().append(newString).reverse().toString());
	}
	
}
