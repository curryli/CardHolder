package com.up.cardholderTag.carTagEngine;

/*
                              _oo0oo_
                             088888880
                             88" . "88
                             (| -_- |)
	                          0\ = /0
                           ___/'---'\___
                       .' \\\\|     |// '.
                      / \\\\|||  :  |||// \\
                       /_ ||||| -:- |||||- \\
                     | | \\\\\\  -  /// |   |
                      | \_|  ''\---/''  |_/ |
                      \  .-\__  '-'  __/-.  /
                    ___'. .'  /--.--\  '. .'___
                 ."" '<  '.___\_<|>_/___.' >'  "".
                | | : '-  \'.;'\ _ /';.'/ - ' : | |
                \  \ '_.   \_ __\ /__ _/   .-' /  /
            ====='-.____'.___ \_____/___.-'____.-'=====
                              '=---='


		  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
					   佛祖保佑        iii   永无bug
*/

import java.io.BufferedReader;
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
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.up.cardholderTag.carTagEngine.CarTravellerFeature.ConvertReducer.TransItem;
import com.up.cardholderTag.domain.CardBinInfo;
import com.up.cardholderTag.domain.CardBinTree;
import com.up.util.Constant;
import com.up.util.MD5;
import com.up.util.TagUtility;

public class CarTagReducer extends Reducer<Text,Text,Text,Text>{
	private static CardBinTree globelCardBinTree = new CardBinTree();
	private String inCardBinPath;
	
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		inCardBinPath = conf.get("inCardBinPath").trim();
		
//		if (inCardBinPath.substring(inCardBinPath.length() - 1).equals("/")) {
//			inCardBinPath = inCardBinPath + "part-r-00000";
//		} else {
//			inCardBinPath = inCardBinPath + "/part-r-00000";
//		}
		
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(inCardBinPath));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				hdfsInStream));
		String tmpString = null;
		while ((tmpString = reader.readLine()) != null) {
			CardBinInfo CardBin = new CardBinInfo();

			String[] CardInfoBin = tmpString.split("\t");
			CardBin.setCardBin(CardInfoBin[0]);

			String[] CardInfoBinContext = CardInfoBin[1].split(Constant.separator_1);
			CardBin.setCardATTR(CardInfoBinContext[0]);
			CardBin.setCardBrand(CardInfoBinContext[1]);
			CardBin.setCardProduct(CardInfoBinContext[2]);
			CardBin.setCardLevel(CardInfoBinContext[3]);
			CardBin.setCardMedia(CardInfoBinContext[4]);

			globelCardBinTree.addNode(CardBin);
		}
	}
	
	class TransItem{
		
		private String cardNum;						//卡号
		private String md5;							//MD5
		private String mcc;							//mcc
		private String mccName;						//mcc名称
		private String mchntName;					//商户名
		private String time;						//消费时间
		private String month;						//月份
		private float trans_at;						//消费金额
		private String acptIns;						//受理机构
		private String locationCd;					//地区代码
		private String city;						//城市
		private String province;					//省份
		private String realProvince;				//真实省份
		
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
			this.month = time.substring(4,6);
			this.trans_at = Float.parseFloat(tokens[15].trim())/100;
			this.acptIns = tokens[7].trim();
			this.locationCd = this.acptIns.substring(4,8);				//地区代码
			this.city = Constant.getCity(this.locationCd);
			this.province = Constant.getProvince(this.city);
			this.realProvince = Constant.getCityKey(this.mchntName);
			
			//修正异地收单商户的所在地
			/*
			if(!realProvince.equals("null")){
				if(!realProvince.equals(province))
					this.province = this.realProvince;
			}*/
			if(realProvince.equals("null"))
				this.realProvince = this.province;
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
		
		public String getMonth() {
			return month;
		}

		public void setMonth(String month) {
			this.month = month;
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
		
		public String getLocationCd() {
			return locationCd;
		}

		public void setLocationCd(String locationCd) {
			this.locationCd = locationCd;
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
		String[] keyTokens = key.toString().split(Constant.separator_1);
		
		String card_num = keyTokens[0].toString();
		String card_md5 = MD5.GetMD5Code(card_num);						//MD5
		String iss_ins_code = keyTokens[1].toString();					//发卡机构代码
		String iss_ins_name = Constant.getInstitution(iss_ins_code);	//发卡机构名称
		String card_level = "-1";										//卡等级
		String confidence = TagUtility.LOW_CONFIDENCE;					//置信度
		int is_new_car = 0;							//是否近一年新买车
		String purchase_month = TagUtility.NULL;				//购车月份
		int refuel_times = 0;						//年加油次数
		float refuel_sum = 0f;						//年加油总金额
		float avg_refuel_amount = 0f;				//次均加油金额
		float low_refuel_times_pct = 0f;			//低排量加油次数占比 
		float media_refuel_times_pct = 0f;			//中排量加油次数占比
		float high_refuel_times_pct = 0f;			//大排量加油次数占比
		int abnormal_refuel_times = 0;				//非正常加油次数
		int fuel_card_times = 0;					//年购买油卡次数
		String refuel_loc_cd = TagUtility.NULL;					//主要加油地区代码
		String refuel_loc_city = TagUtility.NULL;				//主要加油地区（市）
		String refuel_loc_province = TagUtility.NULL;			//主要加油地区（省）
		String refuel_record = TagUtility.NULL;					//加油记录（时间+商户名+金额）  ++++++++++++++++++++++++
		int is_other_city_refuel = 0;								//是否异地加油
		int is_other_province_refuel = 0;							//是否异省加油
		String often_refuel_time = TagUtility.NULL;					//最常加油时段(工作日)      
		String refuel_time_record = TagUtility.NULL;				//各时段加油次数记录
		String often_refuel_day = TagUtility.NULL;					//最常加油日期（星期）
		String refuel_day_record = TagUtility.NULL;					//各日期加油次数记录（星期）
		int avg_refuel_day_interval = -1;							//平均加油时间间隔（天）
		int long_refuel_day_interval = -1;							//最长加油时间间隔
		int short_refuel_day_interval = 365;						//最短加油时间间隔
		String often_refuel_amount_interval = TagUtility.NULL;		//最常加油消费价格区间	
		String refule_amount_interval_record = TagUtility.NULL;		//加油消费价格区间记录
		String often_refuel_mchnt = TagUtility.NULL;				//最常加油商户
		String often_refuel_brand = TagUtility.NULL;				//最常加油品牌
		String often_fuelCard_mchnt = TagUtility.NULL;			//最常购买油卡商户
		String often_fuelCard_brand = TagUtility.NULL;			//最常购买油卡品牌			
		int is_fraud = 0;										//是否有套现嫌疑
		float maintain_sum = 0f;					//年维保总金额
		int maintain_times = 0;						//年维保次数	
		float avg_maintain_amount = 0f;				//次均维保金额（去掉最高）
		float agency_sum_pct = 0f;					//经销商维保金额占比
		float not_agency_sum_pct = 0f;				//非经销商维保金额占比
		float lacquer_sum_pct = 0f;					//汽车喷漆金额占比
		float wash_sum_pct = 0f;					//洗车金额占比
		float agency_times_pct = 0f;				//经销商维保次数占比
		float not_agency_times_pct = 0f;			//非经销商维保次数占比
		float lacquer_times_pct = 0f;				//汽车喷漆次数占比
		float wash_times_pct = 0f;					//洗车次数占比
		String maintain_loc_cd = TagUtility.NULL;				//主要维保地区代码
		String maintain_city = TagUtility.NULL;					//主要维保地区（市）
		String maintain_province = TagUtility.NULL;				//主要维保地区（省）
		
		float parking_sum = 0f;						//停车费总额
		int parking_times = 0;						//停车费次数
		float toll_sum = 0f;						//通行费总额
		int toll_times = 0;							//通行费次数
		
		float rent_sum = 0f;						//年租车总金额
		int rent_times = 0;							//年租车总次数
		int is_other_city_rent = 0;					//是否异地租过车
		int is_other_province_rent = 0;				//是否异省租过车
		String rent_record = TagUtility.NULL;		//年租车记录    <时间，城市，省份>
		
		int drive_degree = 0;						//自驾狂热度
		String drive_record = TagUtility.NULL;		//自驾记录     <城市，所属省份，次数>
		int drive_city_count = 0;					//行车城市个数
		int drive_province_count = 0;				//行车省份个数
		
		
		//=============================== 计算开始 ========================================
		StringBuilder sb = new StringBuilder();
		
		// 判断卡等级 
		CardBinInfo cardBin = globelCardBinTree.traverseTree(card_num.substring(2));
		if (cardBin == null) {
			card_level = "-1";
		} else {
			card_level = cardBin.getCardLevel().toString();
		}
		
		
		ArrayList<TransItem> list = new ArrayList<TransItem>();
		for(Text line: lines){
			
			TransItem item = new TransItem(line.toString());
			list.add(item);
			
			if(TagUtility.HIGH_CONF_SET.contains(item.getMcc()))			//计算置信度
				confidence = TagUtility.HIGH_CONFIDENCE;
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
					if(date1.getTime()>date2.getTime())
						flag=0;
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

		
		
		//加油类交易列表
		ArrayList<TransItem> refuelList = new ArrayList<TransItem>();
		//维保类交易列表
		ArrayList<TransItem> maintainList = new ArrayList<TransItem>();
		//停车过路类交易列表
		ArrayList<TransItem> parkingList = new ArrayList<TransItem>();
		//租车类交易列表
		ArrayList<TransItem> rentList = new ArrayList<TransItem>();
		//自驾类交易列表
		ArrayList<TransItem> driveList = new ArrayList<TransItem>();
		
		for(int i=0; i<list.size(); i++){
			
			//计算是否近一年新买车、可能购车年份
			if(list.get(i).getMcc().equals("5511") || list.get(i).getMcc().equals("5521") || list.get(i).getMcc().equals("5721"))
			{
				if(list.get(i).getTrans_at()>TagUtility.PURCHASE_CAR_THRESHOLD)
				{
					is_new_car = 1;
					purchase_month = list.get(i).getMonth();
				}
			}
			
			if(list.get(i).getMcc().equals("5541") || list.get(i).getMcc().equals("5542"))
				refuelList.add(list.get(i));
			else if(TagUtility.MAINTAIN_MCC_SET.contains(list.get(i).getMcc()))
				maintainList.add(list.get(i));
			else if(list.get(i).getMcc().equals("7523") || list.get(i).getMcc().equals("4784"))
				parkingList.add(list.get(i));
			else if(list.get(i).getMcc().equals("7512"))
				rentList.add(list.get(i));
			//自驾类相关交易
			if(TagUtility.isDriveTrans(list.get(i).getMcc()))
				driveList.add(list.get(i));
		}
		
		
		//===================== 计算加油类基本特征  ===========================================================
		if(refuelList.size()>0){
			int low_refuel_times = 0;
			int media_refuel_times = 0;
			int high_refuel_times = 0;
			float max_refuel_amount = 0;					//最大加油花费
			HashMap<String, Integer> locMap = new HashMap<String, Integer>();
			HashMap<String, Integer> refuelMchntMap = new HashMap<String, Integer>();
			HashMap<String, Integer> fuelCardMchntMap = new HashMap<String, Integer>();
			ArrayList<String> timeList = new ArrayList<String>();
			StringBuilder refuelSb = new StringBuilder();							//加油记录
			HashMap<String, Integer> amountIntervalMap = new HashMap<String, Integer>();		//记录加油价格区间出现的次数
			for(TransItem item: refuelList){
				//排除虚假交易以及油卡交易
				if((item.getTrans_at() < TagUtility.FAKE_REFUEL_THRESHOLD) && (item.getTrans_at()%1000)!=0){
					refuel_times++;
					refuel_sum += item.getTrans_at();
					
					refuelSb.append(item.getTime()+Constant.separator_3);
					refuelSb.append(item.getMchntName()+Constant.separator_3);
					refuelSb.append(item.getTrans_at());
					refuelSb.append(Constant.separator_2);
					
					//取最大加油价格
					if(item.getTrans_at()>max_refuel_amount)
						max_refuel_amount = item.getTrans_at();
					
					if(item.getTrans_at() <= TagUtility.LOW_REFUEL_THRESHOLD)
						low_refuel_times++;
					else if(item.getTrans_at()>TagUtility.LOW_REFUEL_THRESHOLD && item.getTrans_at()<=TagUtility.MEDIA_REFUEL_THRESHOLD)
						media_refuel_times++;
					else if(item.getTrans_at()>TagUtility.MEDIA_REFUEL_THRESHOLD && item.getTrans_at()<=TagUtility.FAKE_REFUEL_THRESHOLD)
						high_refuel_times++;
					
					//统计各个地区出现的次数
					if(locMap.get(item.getLocationCd())!=null){
						locMap.put(item.getLocationCd(), locMap.get(item.getLocationCd())+1);
					}
					else
						locMap.put(item.getLocationCd(), 1);
					
					//统计各个加油站商户出现的次数
					if(refuelMchntMap.get(item.getMchntName())!=null){
						refuelMchntMap.put(item.getMchntName(), refuelMchntMap.get(item.getMchntName())+1);
					}
					else
						refuelMchntMap.put(item.getMchntName(), 1);
					
					//计算加油价格区间分布                   &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
					String amountIntervalName = getAmountInterval(item.getTrans_at());
					if(amountIntervalMap.get(amountIntervalName)!=null){
						int intervaleTimes = amountIntervalMap.get(amountIntervalName);
						amountIntervalMap.put(amountIntervalName, intervaleTimes+1);
					}
					else
						amountIntervalMap.put(amountIntervalName, 1);
					
				}
				else if(item.getTrans_at()%1000==0)					//购买油卡
				{
					fuel_card_times++;
					
					//统计各个地区出现的次数
					if(locMap.get(item.getLocationCd())!=null){
						locMap.put(item.getLocationCd(), locMap.get(item.getLocationCd())+1);
					}
					else
						locMap.put(item.getLocationCd(), 1);
					
					//统计各个购买油卡商户出现的次数
					if(fuelCardMchntMap.get(item.getMchntName())!=null){
						fuelCardMchntMap.put(item.getMchntName(), fuelCardMchntMap.get(item.getMchntName())+1);
					}
					else
						fuelCardMchntMap.put(item.getMchntName(), 1);
				}
				else if(item.getTrans_at() >= TagUtility.FAKE_REFUEL_THRESHOLD)		//非正常交易
				{
					abnormal_refuel_times++;
					
					//统计各个地区出现的次数
					if(locMap.get(item.getLocationCd())!=null){
						locMap.put(item.getLocationCd(), locMap.get(item.getLocationCd())+1);
					}
					else
						locMap.put(item.getLocationCd(), 1);
				}
				timeList.add(item.getTime());
			}
			
			//加油记录
			if(refuelSb.length()>Constant.separator_2.length())
				refuelSb.setLength(refuelSb.length()-Constant.separator_2.length());
			refuel_record = refuelSb.toString();
			
			if(refuel_times>0){
				//去掉最高计算平均加油价格
				if(refuel_times>=3)
					avg_refuel_amount = (refuel_sum - max_refuel_amount)/(refuel_times - 1);
				else
					avg_refuel_amount = refuel_sum/refuel_times;
				
				low_refuel_times_pct = (float)low_refuel_times/refuel_times;
				media_refuel_times_pct = (float)media_refuel_times/refuel_times;
				high_refuel_times_pct = (float)high_refuel_times/refuel_times;
			}
			
			int maxLocTimes = 0;
			HashSet<String> refuelProSet = new HashSet<String>();		//记录出现过的省份
			Iterator it = locMap.keySet().iterator();
			while(it.hasNext()){
				String temp = (String)it.next();
				if(locMap.get(temp)>maxLocTimes){
					refuel_loc_cd = temp;
					maxLocTimes = locMap.get(temp);
				}
				refuelProSet.add(Constant.getProvince(temp));   		//记录出现过的省份
			}
			//最常加油地区
			refuel_loc_city = Constant.getCity(refuel_loc_cd);
			refuel_loc_province = Constant.getProvince(refuel_loc_city);
			
			//是否异地加过油
			if(locMap.keySet().size()>1)
				is_other_city_refuel = 1;
			
			//是否异省加过油
			if(refuelProSet.size()>1)
				is_other_province_refuel = 1;		
		
			//计算时间段和星期分布
			HashMap<String, Integer> weekdayMap = new HashMap<String, Integer>();
			for(int i=0; i<TagUtility.WEEK_DAYS.length; i++){
				weekdayMap.put(TagUtility.WEEK_DAYS[i], 0);
			}
			HashMap<String, Integer> timeIntervalMap = new HashMap<String, Integer>();
			for(int i=0; i<TagUtility.TIME_INTERVAL_NAME.length; i++){
				timeIntervalMap.put(TagUtility.TIME_INTERVAL_NAME[i], 0);
			}
			Calendar calendar = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
			for(String timeItem: timeList){
				try {
					Date date = sdf.parse(timeItem);
					calendar.setTime(date);
					String weekday = TagUtility.WEEK_DAYS[calendar.get(calendar.DAY_OF_WEEK)-1];		//计算星期几
					String timeInterval = getTimeIntervelName(timeItem.substring(8,10));
					weekdayMap.put(weekday, weekdayMap.get(weekday)+1);									//计算每个星期出现的次数
					
					//只计算工作日加油的时间段分布，非工作日无所谓
					if((calendar.get(Calendar.DAY_OF_WEEK)!=Calendar.SATURDAY) && (calendar.get(Calendar.DAY_OF_WEEK)!=Calendar.SUNDAY))
						timeIntervalMap.put(timeInterval, timeIntervalMap.get(timeInterval)+1);				//计算每个时间段出现 的次数
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			
			//计算加油时段(工作日)、各时段加油次数记录
			StringBuilder recordSb = new StringBuilder(); 
			often_refuel_time = getRecordFromMap(timeIntervalMap, recordSb);
			refuel_time_record = recordSb.toString();
			
			//最常加油日期（星期）、各日期加油次数记录
			recordSb = new StringBuilder();
			often_refuel_day = getRecordFromMap(weekdayMap,recordSb);
			refuel_day_record = recordSb.toString();
			
			//计算加油时间间隔（平均、最长、最短）
			int totalInterval = 0;		
			if(timeList.size()>1){
				for(int i = timeList.size()-1; i>0; i--){
					int dayInterval = getDayInterval(sdf, timeList.get(i),timeList.get(i-1));
					if(dayInterval<0)
						dayInterval = Math.abs(dayInterval);
					totalInterval += dayInterval;
					if(dayInterval > long_refuel_day_interval)
						long_refuel_day_interval = dayInterval;
					if(dayInterval < short_refuel_day_interval)
						short_refuel_day_interval = dayInterval;
				}
				avg_refuel_day_interval = totalInterval/(timeList.size()-1);
			}
			
			
			//计算最常加油商户
			int maxTimes = 0;
			if(refuelMchntMap.keySet().size()>0){				//如果发生过加油消费
				it = refuelMchntMap.keySet().iterator();
				while(it.hasNext()){
					String mapKey = (String)it.next();
					int mapValue = refuelMchntMap.get(mapKey);
					if(mapValue>maxTimes){
						maxTimes = mapValue;
						often_refuel_mchnt = mapKey;
					}
				}
				//最常加油品牌
				often_refuel_brand = TagUtility.getRefuelBrand(often_refuel_mchnt);
			}
						
			//计算最常购买油卡商户
			maxTimes = 0;
			if(fuelCardMchntMap.keySet().size()>0){				//如果购买过油卡
				it = fuelCardMchntMap.keySet().iterator();
				while(it.hasNext()){
					String mapKey = (String)it.next();
					int mapValue = fuelCardMchntMap.get(mapKey);
					if(mapValue>maxTimes){
						maxTimes = mapValue;
						often_fuelCard_mchnt = mapKey;
					}
				}
			}
			//最常购买油卡品牌
			if(!often_fuelCard_mchnt.equals(TagUtility.NULL))
				often_fuelCard_brand = TagUtility.getRefuelBrand(often_fuelCard_mchnt);
			
			//是否有套现嫌疑
			if(abnormal_refuel_times>0)
				is_fraud = 1;
			
			//计算加油价格区间分布    &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
			if(amountIntervalMap.keySet().size()>0){
				StringBuilder intervalSb = new StringBuilder();
				Iterator intervalIt = amountIntervalMap.keySet().iterator();
				maxTimes = 0;
				while(intervalIt.hasNext()){
					String mapKey = (String)intervalIt.next();
					int mapValue = amountIntervalMap.get(mapKey);
					if(mapValue>maxTimes){
						maxTimes = mapValue;
						often_refuel_amount_interval = mapKey;
					}
					intervalSb.append(mapKey+Constant.separator_3);
					intervalSb.append(mapValue);
					intervalSb.append(Constant.separator_2);
				}
				intervalSb.setLength(intervalSb.length()-Constant.separator_2.length());
				refule_amount_interval_record = intervalSb.toString();
			}
			
		}
		//=====================end 计算加油类基本特征  ===========================================================
	
		//===================== 计算维保类基本特征  ===========================================================
		if(maintainList.size()>0){
			float agency_sum = 0f;
			int agency_times = 0;
			float not_agency_sum = 0f;
			int not_agency_times = 0;
			float lacquer_sum = 0f;
			int lacquer_times = 0;
			float wash_sum = 0f;
			int wash_times = 0;
			HashMap<String, Integer> locMap = new HashMap<String, Integer>();
			float max_fee = 0f;				//单次最高维保金额
			for(TransItem item: maintainList){
				maintain_times +=1;
				maintain_sum += item.getTrans_at();
				if(item.getTrans_at()>max_fee)
					max_fee = item.getTrans_at();
				
				if(item.getMcc().equals("5511") || item.getMcc().equals("5521")){
					agency_times+=1;
					agency_sum += item.getTrans_at();
				}
				
				//5532	 汽车轮胎经销商
				//5533	汽车零配件商店
				//7531	车体维修店
				//7534	 轮胎翻新维修店
				//7538 	汽车服务商店
				else if(item.getMcc().equals("5532") || item.getMcc().equals("5533") || item.getMcc().equals("7531") || item.getMcc().equals("7534") || item.getMcc().equals("7538"))
				{
					not_agency_times+=1;
					not_agency_sum += item.getTrans_at();
				}
				//7535 	汽车喷漆店
				else if(item.getMcc().equals("7535"))
				{
					lacquer_times+=1;
					lacquer_sum += item.getTrans_at();
				}
				//7542 洗车
				else if(item.getMcc().equals("7542"))
				{
					wash_times+=1;
					wash_sum += item.getTrans_at();
				}
				
				//统计各个地区出现的次数
				if(locMap.get(item.getLocationCd())!=null){
					locMap.put(item.getLocationCd(), locMap.get(item.getLocationCd())+1);
				}
				else
					locMap.put(item.getLocationCd(), 1);
			}
			agency_sum_pct = agency_sum/maintain_sum;
			not_agency_sum_pct = not_agency_sum/maintain_sum;
			lacquer_sum_pct = lacquer_sum/maintain_sum;
			wash_sum_pct = wash_sum/maintain_sum;
			
			agency_times_pct = (float)agency_times/maintain_times;
			not_agency_times_pct = (float)not_agency_times/maintain_times;
			lacquer_times_pct = (float)lacquer_times/maintain_times;
			wash_times_pct = (float)wash_times/maintain_times;
			
			if(maintainList.size()>2)
				avg_maintain_amount = (maintain_sum-max_fee)/(maintain_times-1);		//去除最大值奇异点的影响（可能是购车交易）
			else
				avg_maintain_amount = (maintain_sum)/(maintain_times);
			
			int maxLocTimes = 0;
			Iterator maintainIt = locMap.keySet().iterator();
			while(maintainIt.hasNext()){
				String temp = (String)maintainIt.next();
				if(locMap.get(temp)>maxLocTimes)
					maintain_loc_cd = temp;
			}
			maintain_city = Constant.getCity(maintain_loc_cd);
			maintain_province = Constant.getProvince(maintain_city);
			
		}
		//===================== end 计算维保类基本特征  ===========================================================
		
		//===================== 计算通行停车类基本特征  ===========================================================
		if(parkingList.size()>0){
			for(TransItem item: parkingList){
				if(item.getMcc().equals("4784")){			//路桥通行费
					toll_times ++;
					toll_sum += item.getTrans_at();
				}
				else if(item.getMcc().equals("7523")){		//停车场
					parking_times ++;
					parking_sum += item.getTrans_at();
				}
			}
		}
		//===================== end 计算通行停车类基本特征  ===========================================================
		
		//===================== 计算租车类基本特征  ===========================================================
		if(rentList.size()>0){
			StringBuilder rentSb = new StringBuilder();
			HashSet<String> citySet = new HashSet<String>();
			HashSet<String> provinceSet = new HashSet<String>();
			for(TransItem item: rentList){
				rent_times++;
				rent_sum += item.getTrans_at();
				
				citySet.add(item.getCity());
				provinceSet.add(item.getProvince());
				rentSb.append(item.getTime()+Constant.separator_3);
				rentSb.append(item.getCity()+Constant.separator_3);
				rentSb.append(item.getProvince());
				rentSb.append(Constant.separator_2);
			}
			citySet.add(refuel_loc_city);
			provinceSet.add(refuel_loc_province);
			//租车记录
			rentSb.setLength(rentSb.length()-Constant.separator_2.length());
			rent_record = rentSb.toString();
			//是否异地租过车
			if(citySet.size()>1)
				is_other_city_rent = 1;
			//是否异省租过车
			if(provinceSet.size()>1)
				is_other_province_rent = 1;
		}
		
		//===================== end 计算租车类基本特征  ===========================================================
		
		//===================== 计算自驾狂热特征=================================================
		if(driveList.size()>0){
			StringBuilder driveSb = new StringBuilder();
			HashMap<String, Integer> driveCityMap = new HashMap<String , Integer>();
			HashSet<String> driveProSet = new HashSet<String>();
			
			for(TransItem item: driveList){
				if(driveCityMap.get(item.getCity())!=null){
					int cityTimes = driveCityMap.get(item.getCity());
					driveCityMap.put(item.getCity(), cityTimes+1);
				}
				else{
					driveCityMap.put(item.getCity(), 1);
				}
			}
			
			Iterator<String> it = driveCityMap.keySet().iterator();
			while(it.hasNext()){
				String cityKey = it.next();
				String province = Constant.getProvince(cityKey);
				driveProSet.add(province);
				driveSb.append(cityKey + Constant.separator_3);						//城市
				driveSb.append(province + Constant.separator_3);					//省份
				driveSb.append(driveCityMap.get(cityKey));							//次数
				driveSb.append(Constant.separator_2);
			}
			driveSb.setLength(driveSb.length()-Constant.separator_2.length());
			drive_record = driveSb.toString();										//自驾记录           <城市，所属省份，次数>
			
			drive_city_count = driveCityMap.keySet().size();						//自驾城市个数
			drive_province_count = driveProSet.size();								//自驾省份个数
			
			//计算自驾狂热度
			if(drive_city_count>0)
				drive_degree = 1;
			if(is_other_province_refuel == 0 && is_other_city_refuel == 1 )			//自驾狂热度
				drive_degree = 2;	
			if(drive_province_count>1)
				drive_degree = 3;
			if(is_other_city_rent == 1)
				drive_degree = 3;
		}
		
		//===================== end 计算自驾狂热特征=================================================
		sb.append(card_num+Constant.separator_1);
		sb.append(card_md5+Constant.separator_1);									//MD5
		sb.append(iss_ins_code+Constant.separator_1);								//发卡机构代码
		sb.append(iss_ins_name+Constant.separator_1);								//发卡机构名称
		sb.append(card_level+Constant.separator_1);									//卡等级
		sb.append(confidence+Constant.separator_1);									//置信度
		sb.append(is_new_car+Constant.separator_1);									//是否近一年新买车
		sb.append(purchase_month+Constant.separator_1);								//购车月份
		sb.append(refuel_times+Constant.separator_1);								//年加油次数
		sb.append(refuel_sum+Constant.separator_1);									//年加油总金额
		sb.append(avg_refuel_amount+Constant.separator_1);							//次均加油金额
		sb.append(low_refuel_times_pct+Constant.separator_1);						//低排量加油次数占比 
		sb.append(media_refuel_times_pct+Constant.separator_1);						//中排量加油次数占比
		sb.append(high_refuel_times_pct+Constant.separator_1);						//大排量加油次数占比
		sb.append(abnormal_refuel_times+Constant.separator_1);						//非正常加油次数
		sb.append(fuel_card_times+Constant.separator_1);							//年购买油卡次数
		sb.append(refuel_loc_cd+Constant.separator_1);								//主要加油地区代码
		sb.append(refuel_loc_city+Constant.separator_1);							//主要加油地区(市)
		sb.append(refuel_loc_province+Constant.separator_1);						//主要加油地区(省)
		sb.append(refuel_record+Constant.separator_1);								//加油记录（时间+地点+金额）
		sb.append(is_other_city_refuel+Constant.separator_1);						//是否异地加过油
		sb.append(is_other_province_refuel+Constant.separator_1);					//是否异省加过油
		sb.append(often_refuel_time+Constant.separator_1);							//最常加油时段（工作日）
		sb.append(refuel_time_record+Constant.separator_1);							//各时段加油次数记录
		sb.append(often_refuel_day+Constant.separator_1);							//最常加油日期（星期）
		sb.append(refuel_day_record+Constant.separator_1);							//各日期加油次数记录（星期）
		sb.append(avg_refuel_day_interval+Constant.separator_1);					//平均加油时间间隔（天）
		sb.append(long_refuel_day_interval+Constant.separator_1);					//最长加油时间间隔
		sb.append(short_refuel_day_interval+Constant.separator_1);					//最短加油时间间隔
		sb.append(often_refuel_amount_interval+Constant.separator_1);				//最常加油消费价格区间
		sb.append(refule_amount_interval_record+Constant.separator_1);				//加油消费价格区间记录
		sb.append(often_refuel_mchnt+Constant.separator_1);							//最常加油商户
		sb.append(often_refuel_brand+Constant.separator_1);							//最常加油商户品牌
		sb.append(often_fuelCard_mchnt+Constant.separator_1);						//最常购买油卡商户
		sb.append(often_fuelCard_brand+Constant.separator_1);						//最常购买油卡品牌
		sb.append(is_fraud+Constant.separator_1);									//是否有套现嫌疑
		sb.append(maintain_sum+Constant.separator_1);								//年维保总金额
		sb.append(maintain_times+Constant.separator_1);								//年维保次数
		sb.append(avg_maintain_amount+Constant.separator_1);						//次均维保花费（去掉最高最低取平均）
		sb.append(agency_sum_pct+Constant.separator_1);								//经销商维保金额占比
		sb.append(not_agency_sum_pct+Constant.separator_1);							//非经销商维保金额占比
		sb.append(lacquer_sum_pct+Constant.separator_1);							//汽车喷漆金额占比
		sb.append(wash_sum_pct+Constant.separator_1);								//洗车金额占比
		sb.append(agency_times_pct+Constant.separator_1);							//经销商维保次数占比
		sb.append(not_agency_times_pct+Constant.separator_1);						//非经销商维保次数占比
		sb.append(lacquer_times_pct+Constant.separator_1);							//汽车喷漆次数占比
		sb.append(wash_times_pct+Constant.separator_1);								//洗车次数占比	
		sb.append(maintain_loc_cd+Constant.separator_1);							//主要维保地区代码
		sb.append(maintain_city+Constant.separator_1);								//主要维保地区（市）
		sb.append(maintain_province+Constant.separator_1);							//主要维保地区（省）
		sb.append(parking_sum+Constant.separator_1);								//停车费总额
		sb.append(parking_times+Constant.separator_1);								//停车费次数
		sb.append(toll_sum+Constant.separator_1);									//通行费总额
		sb.append(toll_times+Constant.separator_1);									//通行费次数
		sb.append(rent_sum+Constant.separator_1);									//年租车总金额
		sb.append(rent_times+Constant.separator_1);									//年租车总次数
		sb.append(is_other_city_rent+Constant.separator_1);							//是否异地租过车
		sb.append(is_other_province_rent+Constant.separator_1);						//是否异省租过车
		sb.append(rent_record+Constant.separator_1);								//年租车记录  <时间，城市 ，省份>
		sb.append(drive_degree+Constant.separator_1);								//自驾狂热度
		sb.append(drive_record+Constant.separator_1);								//自加记录
		sb.append(drive_city_count+Constant.separator_1);							//行车城市个数
		sb.append(drive_province_count);											//行车省份记录
		
		context.write(new Text(sb.toString()), new Text(""));
	}
	
	/**
	 * 计算当前时刻属于哪个时段
	 * */
	public String getTimeIntervelName(String hour){
		int str = Integer.parseInt(hour);
		for(int i=TagUtility.TIME_INTERVAL.length-1; i >0 ; i--){
			if(str>=TagUtility.TIME_INTERVAL[i-1] && str<TagUtility.TIME_INTERVAL[i]){
				return TagUtility.TIME_INTERVAL_NAME[i];
			}
		}
		return TagUtility.TIME_INTERVAL_NAME[0];
	}
	
	/**
	 * 将哈希表里的次数数据 连成记录，存在stringBuilder里，并返回次数最大的key
	 * */
	public String getRecordFromMap(HashMap<String, Integer> map, StringBuilder sb){
		int max = 0;
		String maxString = "";
		Iterator it = map.keySet().iterator();
		while(it.hasNext()){
			String key = (String)it.next();
			int count = map.get(key);
			if(count>max){
				max = count;
				maxString = key;
			}
			sb.append(key+Constant.separator_3+count);
			sb.append(Constant.separator_2);
		}
		sb.setLength(sb.length()-Constant.separator_2.length());
		return maxString;
	}
	
	/**
	 * 计算两个日期之间的时间间隔（天）
	 * */
	public static int getDayInterval(SimpleDateFormat sdf, String time1, String time2){
		Date date1;
		Date date2;
		try {
			date1 = sdf.parse(time1);
			date2 = sdf.parse(time2);
			return (int)Math.abs((date1.getTime()-date2.getTime()))/(1000*60*60*24);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 计算当前的消费金额属于哪个区间
	 * 0-200，200-300，300-400，400-500，500-600，600-700，700-800，800-900，900-1000，1000+
	 * */
	public String getAmountInterval(float amount){
		if(amount>TagUtility.AMOUNT_INTERVAL[TagUtility.AMOUNT_INTERVAL.length-1])
			return TagUtility.AMOUNT_INTERVAL_NAME[TagUtility.AMOUNT_INTERVAL_NAME.length-1];
		for(int i = TagUtility.AMOUNT_INTERVAL.length-1; i>0; i--)
		{
			if(amount>=TagUtility.AMOUNT_INTERVAL[i-1] && amount<TagUtility.AMOUNT_INTERVAL[i])
				return TagUtility.AMOUNT_INTERVAL_NAME[i];
		}	
		
		return TagUtility.AMOUNT_INTERVAL_NAME[0];
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
