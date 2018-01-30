package com.up.merchant.customerFlow;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.up.merchant.customerFlow.CaclCustomerFlowFeature.ConvertReducer.BrandInfo;
import com.up.util.Constant;


public class ResultAnalysis {
	public HashMap<String, String> LOCATION_MAP;
	
	public ResultAnalysis(){
		String filename = "p://0725/location";
		LOCATION_MAP = new HashMap<String, String>();
		String encoding="GBK";
		
		String str = null;
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), encoding));
			while((str=br.readLine())!=null){
				String[] tokens = str.split("\\s+");
				LOCATION_MAP.put(tokens[0].trim(), tokens[1].trim());
				//System.out.println("LOCATION_MAP.put(\""+tokens[0]+"\","+tokens[1]+");");
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void readFile(String filename) throws Exception{
		int count = 0;
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
		File outputFile = new File("relation.new");
		if(outputFile.exists())
			outputFile.delete();
		
		FileOutputStream fo = new FileOutputStream("relation.new");
		PrintWriter writer = new PrintWriter(fo);
		String str = null;
		try{
			while((str=br.readLine())!=null){
				count++;
				String[] tokens = str.split(Constant.separator_1);
				String brandName = tokens[0].trim();
				if(!brandName.equals("锦江之星连锁酒店"))
					continue;
				
				String onceOverallFeature = tokens[1].trim();
				String moreOverallFeature = tokens[2].trim();
				String onceFavoriteFeature = tokens[3].trim();
				String onceNotFavoriteFeature = tokens[4].trim();
				String moreFavoriteFeature = tokens[5].trim();
				String moreNotFavoriteFeature = tokens[6].trim();
				String brandRecognition = tokens[7].trim();
				
				//解析消费一次的客户的特征
				System.out.println("==============费一次的客户的特征==============");
				String[] onceOFTokens = onceOverallFeature.split(Constant.separator_3);
				String oofCount = onceOFTokens[0].trim();
				String oofTimes = onceOFTokens[1].trim();
				String oofAmount = onceOFTokens[2].trim();
				System.out.println("oofCount :" + oofCount);
				System.out.println("oofTimes :" + oofTimes);
				System.out.println("oofAmount :" + oofAmount);
				
				//解析消费多次客户特征，以及回访情况分布
				String[] moreOFTokens = moreOverallFeature.split(Constant.separator_3);
				System.out.println("==============消费多次客户特征，以及回访情况分布==============");
				String mofCount = moreOFTokens[0].trim();
				String mofTimes = moreOFTokens[1].trim();
				String mofAmount = moreOFTokens[2].trim();
				String mofTimes_2 = moreOFTokens[3].trim();
				String mofTimes_3 = moreOFTokens[4].trim();
				String mofTimes_4 = moreOFTokens[5].trim();
				String mofTimes_5 = moreOFTokens[6].trim();
				String mofTimes_6_10 = moreOFTokens[7].trim();
				String mofTimes_lt_10 = moreOFTokens[8].trim();
				System.out.println("mofCount :" + mofCount);
				System.out.println("mofTimes :" + mofTimes);
				System.out.println("mofAmount :" + mofAmount);
				System.out.println("mofTimes_2 :" + mofTimes_2);
				System.out.println("mofTimes_3 :" + mofTimes_3);
				System.out.println("mofTimes_4 :" + mofTimes_4);
				System.out.println("mofTimes_5 :" + mofTimes_5);
				System.out.println("mofTimes_6_10 :" + mofTimes_6_10);
				System.out.println("mofTimes_lt_10 :" + mofTimes_lt_10);
				
				//解析来过1次且同行业只消费1次客户群特征与流向
				System.out.println("==============来过1次且同行业只消费1次客户群特征与流向==============");
				String[] offTokens = onceFavoriteFeature.split(Constant.separator_2);
				ArrayList<String> off_basicFeature = getBasicInfoList(offTokens[0].trim());
				ArrayList<String> off_cardClassFeature = getCardClassMap(offTokens[1].trim());
				ArrayList<String> off_cardAttrFeature = getCardAttrMap(offTokens[2].trim());
				ArrayList<String> off_cardBrandFeature = getCardBrandMap(offTokens[3].trim());
				ArrayList<String> off_cardLevelFeature = getCardLevelMap(offTokens[4].trim());
				ArrayList<InsCdInfo> off_cardInsCdFeature = getInsCdMap(offTokens[5].trim());
				
				//解析来过1次且同行业消费多次客户群特征与流向
				System.out.println("==============来过1次且同行业消费多次客户群特征与流向==============");
				String[] onfTokens = onceNotFavoriteFeature.split(Constant.separator_2);
				ArrayList<String> onf_basicFeature = getBasicInfoList(onfTokens[0].trim());
				ArrayList<String> onf_cardClassFeature = getCardClassMap(onfTokens[1].trim());
				ArrayList<String> onf_cardAttrFeature = getCardAttrMap(onfTokens[2].trim());
				ArrayList<String> onf_cardBrandFeature = getCardBrandMap(onfTokens[3].trim());
				ArrayList<String> onf_cardLevelFeature = getCardLevelMap(onfTokens[4].trim());
				ArrayList<InsCdInfo> onf_cardInsCdFeature = getInsCdMap(offTokens[5].trim());
				ArrayList<String> onf_customerFlowFeatture = getCustomerFlowMap(onfTokens[6].trim());
				
				//解析来过多次且在同行业为首选的客户群特征与流向
				System.out.println("==============来过多次且在同行业为首选的客户群特征与流向==============");
				String[] mffTokens = moreFavoriteFeature.split(Constant.separator_2);
				ArrayList<String> mff_basicFeature = getBasicInfoList(mffTokens[0].trim());
				ArrayList<String> mff_cardClassFeature = getCardClassMap(mffTokens[1].trim());
				ArrayList<String> mff_cardAttrFeature = getCardAttrMap(mffTokens[2].trim());
				ArrayList<String> mff_cardBrandFeature = getCardBrandMap(mffTokens[3].trim());
				ArrayList<String> mff_cardLevelFeature = getCardLevelMap(mffTokens[4].trim());
				ArrayList<InsCdInfo> mff_cardInsCdFeature = getInsCdMap(mffTokens[5].trim());
				ArrayList<String> mff_customerFlowFeatture = getCustomerFlowMap(mffTokens[6].trim());
				
				
				//解析来过多次且在同行业非首选的客户群特征与流向
				System.out.println("==============来过多次且在同行业非首选的客户群特征与流向==============");
				String[] mnfTokens = moreNotFavoriteFeature.split(Constant.separator_2);
				ArrayList<String> mnf_basicFeature = getBasicInfoList(mnfTokens[0].trim());
				ArrayList<String> mnf_cardClassFeature = getCardClassMap(mnfTokens[1].trim());
				ArrayList<String> mnf_cardAttrFeature = getCardAttrMap(mnfTokens[2].trim());
				ArrayList<String> mnf_cardBrandFeature = getCardBrandMap(mnfTokens[3].trim());
				ArrayList<String> mnf_cardLevelFeature = getCardLevelMap(mnfTokens[4].trim());
				ArrayList<InsCdInfo> mnf_cardInsCdFeature = getInsCdMap(mnfTokens[5].trim());
				ArrayList<String> mnf_customerFlowFeature = getCustomerFlowMap(mnfTokens[6].trim());
				
				
				ArrayList<String> brandRecognitionFeature = getBrandRecognition(brandRecognition,Integer.parseInt(oofCount)+Integer.parseInt(mofCount));
				/*
				for(int i = 0; i<tokens.length; i++){
					System.out.println(tokens[i]);
				}*/
				break;
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		br.close();
		writer.flush();
		writer.close();
	}
	
	public ArrayList<String> getBasicInfoList(String line){
		ArrayList<String> list = new ArrayList<String>();
		String[] tokens = line.split(Constant.separator_3);
		list.add(tokens[0].trim());					//该人群总人数
		list.add(tokens[1].trim());					//在本店消费次数
		list.add(tokens[2].trim());					//在本店消费总金额
		list.add(tokens[3].trim());					//该人群在本行业消费总次数
		list.add(tokens[4].trim());					//该人群在本行业消费总金额
		

		System.out.println("该人群总人数 :" + tokens[0].trim());
		System.out.println("在本店消费次数 :" + tokens[1].trim());
		System.out.println("在本店消费总金额 :" + tokens[2].trim());
		System.out.println("该人群在本店消费次均值:" + Double.parseDouble(tokens[2].trim())/Double.parseDouble(tokens[1].trim()));
		System.out.println("该人群在本行业消费总次数 :" + tokens[3].trim());
		System.out.println("该人群在本行业消费总金额 :" + tokens[4].trim());
		System.out.println("该人群在本行业消费次均值:" + Double.parseDouble(tokens[4].trim())/Double.parseDouble(tokens[3].trim()));
		
		return list;
	}
	
	public ArrayList<String> getComplexInfoMap(String line){
		ArrayList<String> map = new ArrayList<String>();
		String[] tokens = line.split(Constant.separator_3);
		for(String bag: tokens){
			map.add(bag);
		}
		
		return map;
	}
	
	public ArrayList<String> getCardClassMap(String line){
		ArrayList<String> map = new ArrayList<String>();
		String[] tokens = line.split(Constant.separator_3);
		for(String bag: tokens){
			map.add(bag);
		}

		System.out.println("\n"+"+++++++++++++++卡种信息+++++++++++++++");
		for(int i=0; i< map.size(); i++){
			System.out.println(map.get(i));
		}
		return map;
	}
	
	public ArrayList<String> getCardAttrMap(String line){
		ArrayList<String> map = new ArrayList<String>();
		String[] tokens = line.split(Constant.separator_3);
		for(String bag: tokens){
			map.add(bag);
		}
		
		System.out.println("\n"+"+++++++++++++++卡属性信息+++++++++++++++");
		for(int i=0; i< map.size(); i++){
			System.out.println(map.get(i));
		}
		return map;
	}
	
	public ArrayList<String> getCardBrandMap(String line){
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
		int total = 0;
		
		ArrayList<String> map = new ArrayList<String>();
		String[] tokens = line.split(Constant.separator_3);
		hashMap.put("其他卡",0);
		for(String bag: tokens){
			map.add(bag);
			
			String[] bagTokens = bag.split(Constant.separator_4);
			total+=Integer.parseInt(bagTokens[1].trim());
			if(bagTokens[0].trim().equals("01"))
				hashMap.put("银联标准卡", Integer.parseInt(bagTokens[1].trim()));
			else if(bagTokens[0].trim().equals("02"))
				hashMap.put("VISA卡", Integer.parseInt(bagTokens[1].trim()));
			else if(bagTokens[0].trim().equals("12")){
				hashMap.put("MASTER卡", Integer.parseInt(bagTokens[1].trim())/2);
				int count = hashMap.get("银联标准卡");
				count += (Integer.parseInt(bagTokens[1].trim())/2);
				hashMap.put("银联标准卡",count);
			}
			else if(bagTokens[0].trim().equals("04"))
				hashMap.put("JCP卡", Integer.parseInt(bagTokens[1].trim()));
			else if(bagTokens[0].trim().equals("06"))
				hashMap.put("美国运通卡", Integer.parseInt(bagTokens[1].trim()));
			else{
				int count = hashMap.get("其他卡");
				count = count + Integer.parseInt(bagTokens[1].trim());
				hashMap.put("其他卡", count);
			}
				
		}
		
		
		
		System.out.println("\n"+"+++++++++++++++卡品牌信息+++++++++++++++");
		for(int i=0; i< map.size(); i++){
			System.out.println(map.get(i));
		}
		System.out.println("\n"+"--------------------");
		System.out.println("total:"+total);
		Iterator it = hashMap.keySet().iterator();
		while(it.hasNext()){
			String name = (String)it.next();
			System.out.println(name+": "+hashMap.get(name)+"    "+Float.parseFloat(hashMap.get(name).toString())/total);
		}
		return map;
	}
	
	public ArrayList<String> getCardLevelMap(String line){
		ArrayList<String> map = new ArrayList<String>();
		String[] tokens = line.split(Constant.separator_3);
		for(String bag: tokens){
			map.add(bag);
		}
		
		System.out.println("\n"+"+++++++++++++++卡等级信息+++++++++++++++");
		for(int i=0; i< map.size(); i++){
			System.out.println(map.get(i));
		}
		return map;
	}
	
	public ArrayList<InsCdInfo> getInsCdMap(String line){
		int total = 0;
		HashMap<String, InsCdInfo> map = new HashMap<String, InsCdInfo>();	
		//HashMap<String, LocInfo> locMap = new HashMap<String, LocInfo>();
		
		String[] tokens = line.split(Constant.separator_3);
		for(String bag: tokens){
			String[] bagTokens = bag.split(Constant.separator_4);
			String insCd = bagTokens[0].substring(0,4);
			//String loc = bagTokens[0].substring(4,8);
			total+=Integer.parseInt(bagTokens[1].trim());
			
			if(map.get(insCd)!=null){
				map.get(insCd).setTimes(map.get(insCd).getTimes()+Integer.parseInt(bagTokens[1].trim()));
			}
			else{
				InsCdInfo info = new InsCdInfo(insCd, Constant.INS_CD_MAP.get(insCd), Integer.parseInt(bagTokens[1].trim()));
				map.put(insCd, info);
			}
			
			/*
			if(locMap.get(loc)!=null){
				locMap.get(loc).setTimes(map.get(loc).getTimes()+Integer.parseInt(bagTokens[1].trim()));
			}
			else{
				LocInfo info = new LocInfo(LOCATION_MAP.get(loc), Integer.parseInt(bagTokens[1].trim()));
				locMap.put(loc, info);
			}*/
		}
		
		ArrayList<InsCdInfo> list = new ArrayList<InsCdInfo>();
		Iterator it = map.keySet().iterator();
		while(it.hasNext()){
			list.add(map.get(it.next()));
		}
		Comparator<InsCdInfo> comparator = new Comparator<InsCdInfo>(){

			@Override
			public int compare(InsCdInfo o1, InsCdInfo o2) {
				// TODO Auto-generated method stub
				return o2.getTimes()-o1.getTimes();
			}
		};
		Collections.sort(list, comparator);
		
		System.out.println("\n"+"+++++++++++++++发卡机构信息+++++++++++++++");
		for(int i=0; i< list.size()&& i<10; i++){
			System.out.println(list.get(i).getInsName()+":"+list.get(i).getTimes()+"   "+Float.parseFloat(String.valueOf(list.get(i).getTimes()))/total);
		}
		
		/*
		ArrayList<LocInfo> locList = new ArrayList<LocInfo>();
		it = locMap.keySet().iterator();
		while(it.hasNext()){
			locList.add(locMap.get(it.next()));
		}
		Comparator<LocInfo> locomparator = new Comparator<LocInfo>(){

			@Override
			public int compare(LocInfo o1, LocInfo o2) {
				// TODO Auto-generated method stub
				return o2.getTimes()-o1.getTimes();
			}
		};
		Collections.sort(locList, locomparator);
		
		System.out.println("\n"+"+++++++++++++++发卡地区信息+++++++++++++++");
		for(int i=0; i< locList.size()&& i<10; i++){
			System.out.println(locList.get(i).getLoc()+":"+locList.get(i).getTimes());
		}
		*/
		
		return list;
	}
	
	public ArrayList<String> getCustomerFlowMap(String line){
		ArrayList<String> map = new ArrayList<String>();
		String[] tokens = line.split(Constant.separator_3);
		for(String bag: tokens){
			map.add(bag);
		}
		
		System.out.println("\n"+"+++++++++++++++客户流向信息+++++++++++++++");
		for(int i=0; i< map.size() && i<10; i++){
			String[] mapTokens = map.get(i).split(Constant.separator_4);
			if(mapTokens[1].equals("1"))
				continue;
			System.out.println(map.get(i));
		}
		return map;
	}
	
	public ArrayList<String> getBrandRecognition(String line, int totalNum){
		ArrayList<String> map = new ArrayList<String>();
		String[] tokens = line.split(Constant.separator_3);
		for(String bag: tokens){
			map.add(bag);
		}
		
		System.out.println("\n"+"+++++++++++++++品牌认可度+++++++++++++++");
		for(int i=0; i< map.size() && i<10; i++){
			String[] mapTokens = map.get(i).split(Constant.separator_4);
			float count = Float.parseFloat(mapTokens[1].trim());
			System.out.println(mapTokens[0].trim()+": "+ mapTokens[1]+"  "+Float.parseFloat(mapTokens[1].trim())/totalNum
					+"  "+ Float.parseFloat(mapTokens[2].trim())/count+"  "+ Float.parseFloat(mapTokens[3].trim())/count
					+" "+ totalNum);
		}
		return map;
	}
	
	public void readInsCd(String filename) throws Exception{
		String encoding="GBK";
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), encoding));
		
		String str = null;
		try{
			while((str=br.readLine())!=null){
				String[] tokens = str.split("\\s+");
				System.out.println("INS_CD_MAP.put(\""+tokens[0]+"\",\""+tokens[2]+"\");");
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	class InsCdInfo{
		private String inscd;
		private String insName;
		private int times;
		
		public InsCdInfo(String insCd, String insName, int times){
			this.inscd = insCd;
			this.insName = insName;
			this.times = times;
		}

		public String getInscd() {
			return inscd;
		}

		public void setInscd(String inscd) {
			this.inscd = inscd;
		}

		public String getInsName() {
			return insName;
		}

		public void setInsName(String insName) {
			this.insName = insName;
		}

		public int getTimes() {
			return times;
		}

		public void setTimes(int times) {
			this.times = times;
		}
	}
	
	class LocInfo{
		private String loc;
		private int times;
		
		public LocInfo(String loc, int times){
			this.loc = loc;
			this.times = times;
		}

		public String getLoc() {
			return loc;
		}

		public void setLoc(String loc) {
			this.loc = loc;
		}

		public int getTimes() {
			return times;
		}

		public void setTimes(int times) {
			this.times = times;
		}
	}
	
	public static void main(String[] args){

		try {
			
			//new ResultAnalysis().readFile("p://0725/brandFeature4");
			new ResultAnalysis().readFile("C://Documents and Settings/wangjun/桌面/0718/brandFeature6");
			//ResultAnalysis.readInsCd("C://Documents and Settings/wangjun/桌面/0718/insCd");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
