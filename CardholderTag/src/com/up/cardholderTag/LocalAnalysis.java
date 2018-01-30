package com.up.cardholderTag;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import com.up.util.Constant;

public class LocalAnalysis {
	
	public static void readCarFile(String filename, String resultAddr){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		HashMap<String, Integer> map;
		String category1 = "小排量车";
		String category2 = "中大排量车";
		String category3 = "客货车或油卡";
		String category4 = "油卡或套现";
		
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			File outputFile = new File(resultAddr);
			if(outputFile.exists())
				outputFile.delete();
			
			FileOutputStream fo = new FileOutputStream(resultAddr);
			PrintWriter writer = new PrintWriter(fo);
			String str = null;
			try{
				StringBuilder sb;
				int count = 0;
				HashMap<String, Integer> amountMap;
				
				while((str=br.readLine())!=null && count<3000){
					String carCategory = "未知";
					boolean isRent = false;
					
					sb = new StringBuilder();
					map = new HashMap<String, Integer>();
					amountMap = new HashMap<String, Integer>();
					amountMap.put(category1, 0);
					amountMap.put(category2, 0);
					amountMap.put(category3, 0);
					amountMap.put(category4, 0);
					
					String[] strTokens = str.split(",");
					sb.append(strTokens[0].trim()+"\n");
					String[] lineTokens = strTokens[1].trim().split(Constant.separator_2);
					for(String record:lineTokens){
						String[] tokens = record.split(Constant.separator_3);
						Date date1 = sdf.parse(tokens[0].trim());
						String date = tokens[0].trim();
						sb.append(date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8)+" "+date.substring(8,10)+":"+date.substring(10,12)+":"+date.substring(12,14)+", ");
						String mcc = tokens[1].trim();
						String mchntName = tokens[2].trim();
						String acptIns = tokens[3].trim();
						float trans_at = Float.parseFloat(tokens[4].trim());
						String city = Constant.getCity(acptIns);
						String province = Constant.getProvince(city);
						
						//过滤异地收单商户
						String realProvince = Constant.getCityKey(mchntName);
						if(!realProvince.equals("null")){
							if(!realProvince.equals(province))
								continue;
						}
						
						//判断是否租过车
						if(mcc.equals("7512"))
							isRent=true;
						
						if(trans_at<=400){
							amountMap.put(category1, amountMap.get(category1)+1);
						}
						else if(trans_at>400 && trans_at<=800){
							amountMap.put(category2, amountMap.get(category2)+1);
						}
						else if(trans_at>800 && trans_at<=3000){
							amountMap.put(category3, amountMap.get(category3)+1);
						}
						else if(trans_at>3000){
							amountMap.put(category4, amountMap.get(category4)+1);
						}
						
						Iterator it = amountMap.keySet().iterator();
						float maxCount = 0;
						while(it.hasNext()){
							String key = (String)it.next();
							if(amountMap.get(key)>maxCount){
								maxCount=amountMap.get(key);
								carCategory = key;
							}
								
						}
						
						
						if(map.get(city)==null)
							map.put(city, 1);
						else{
							int times = map.get(city);
							map.put(city, times++);
						}
						
						sb.append(mcc+"("+Constant.getMccName(mcc)+")"+", ");
						sb.append(city+", "+province+", ");
						sb.append(acptIns + ", ");
						sb.append(mchntName+", ");
						sb.append(trans_at);
						sb.append("\n");
					}

					sb.append(amountMap.get(category1)+","+amountMap.get(category2)+","+amountMap.get(category3)+","+amountMap.get(category4)+","+"\n");
					sb.append(carCategory+"\n");
					
					if(map.size()<1)
						continue;
					else{
						count++;
						System.out.println(sb.toString());
						writer.write(sb.toString()+"\n");
					}
					
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
			br.close();
			writer.flush();
			writer.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void readRentCarFile(String filename, String resultAddr){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		HashMap<String, Integer> map;
		String category1 = "小排量车";
		String category2 = "中大排量车";
		String category3 = "客货车或油卡";
		String category4 = "油卡或套现";
		
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			File outputFile = new File(resultAddr);
			if(outputFile.exists())
				outputFile.delete();
			
			FileOutputStream fo = new FileOutputStream(resultAddr);
			PrintWriter writer = new PrintWriter(fo);
			String str = null;
			try{
				StringBuilder sb;
				int count = 0;
				HashMap<String, Integer> amountMap;
				
				while((str=br.readLine())!=null && count<3000){
					String carCategory = "未知";
					boolean isRent = false;
					
					sb = new StringBuilder();
					map = new HashMap<String, Integer>();
					amountMap = new HashMap<String, Integer>();
					amountMap.put(category1, 0);
					amountMap.put(category2, 0);
					amountMap.put(category3, 0);
					amountMap.put(category4, 0);
					
					String[] strTokens = str.split(",");
					sb.append(strTokens[0].trim()+"\n");
					String[] lineTokens = strTokens[1].trim().split(Constant.separator_2);
					for(String record:lineTokens){
						String[] tokens = record.split(Constant.separator_3);
						Date date1 = sdf.parse(tokens[0].trim());
						String date = tokens[0].trim();
						sb.append(date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8)+" "+date.substring(8,10)+":"+date.substring(10,12)+":"+date.substring(12,14)+", ");
						String mcc = tokens[1].trim();
						String mchntName = tokens[2].trim();
						String acptIns = tokens[3].trim();
						float trans_at = Float.parseFloat(tokens[4].trim());
						String city = Constant.getCity(acptIns);
						String province = Constant.getProvince(city);
						
						//过滤异地收单商户
						String realProvince = Constant.getCityKey(mchntName);
						if(!realProvince.equals("null")){
							if(!realProvince.equals(province))
								continue;
						}
						
						//判断是否租过车
						if(mcc.equals("7512"))
							isRent=true;
						
						if(trans_at<=400){
							amountMap.put(category1, amountMap.get(category1)+1);
						}
						else if(trans_at>400 && trans_at<=800){
							amountMap.put(category2, amountMap.get(category2)+1);
						}
						else if(trans_at>800 && trans_at<=3000){
							amountMap.put(category3, amountMap.get(category3)+1);
						}
						else if(trans_at>3000){
							amountMap.put(category4, amountMap.get(category4)+1);
						}
						
						Iterator it = amountMap.keySet().iterator();
						float maxCount = 0;
						while(it.hasNext()){
							String key = (String)it.next();
							if(amountMap.get(key)>maxCount){
								maxCount=amountMap.get(key);
								carCategory = key;
							}
								
						}
						
						
						if(map.get(city)==null)
							map.put(city, 1);
						else{
							int times = map.get(city);
							map.put(city, times++);
						}
						
						sb.append(mcc+"("+Constant.getMccName(mcc)+")"+", ");
						sb.append(city+", "+province+", ");
						sb.append(acptIns + ", ");
						sb.append(mchntName+", ");
						sb.append(trans_at);
						sb.append("\n");
					}

					sb.append(amountMap.get(category1)+","+amountMap.get(category2)+","+amountMap.get(category3)+","+amountMap.get(category4)+","+"\n");
					sb.append(carCategory+"\n");
					
					if(!isRent)
						continue;
					
					if(map.size()<2)
						continue;
					else{
						count++;
						System.out.println(sb.toString());
						writer.write(sb.toString()+"\n");
					}
					
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
			br.close();
			writer.flush();
			writer.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		//readCarFile("d://result-1113/newTrans","d://carResult2");
		//readCarFile("d://result-1113/newTrans","d://result-1113/carResult");
		readRentCarFile("d://result-1113/newTrans5","d://result-1113/carResult");
	}

}
