package GeneralAPI;
/*
                              _oo0oo_
                             088888880
                             88" . "88
                             (| -_- |)
	                          0\ = /0
                           ___/"---'\___
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import com.up.util.Constant;
import com.up.util.MD5;
import com.up.util.TagUtility;

public class CardReducer extends Reducer<Text,Text,Text,Text>{
	
	private String[] normal_trans_id = {"S22","D22","S20","S81","S80","D80","S21","S35","S13",
			"D13","S54","S50","D46","S51","S49","S46","S83","D56","S56","S67","S73","S71"};
	private String[] abnormal_trans_id = {"V52","V50","V81","V13","D32","S30","S75","E74","E84",
			"S84","Z99","V84","V83","V79","D33","S47","V76","S76","D76","S60","D35","V86","V73","V69"};
	private String[] cash_trans_id = {"S24","S59","S70"};						//取现相关的交易类型
	private String[] ab_cash_trans_id = {"V54"};
	private String[] cater_mcc = {"5441", "5451", "5462", "5499", "5811", "5812", "5813", "5814"};
	private String[] hotel_mcc = {"7011","7012","4511"};
	private String[] mall_mcc = {"7296", "5310", "5311", "5331", "5611", "5621", "5631", "5641", "5651", "5655", 
			"5661", "5681", "5691", "5697", "5698", "5699", "5940", "5941", "5942", "5943", "5948", "5949", "5977",
			"5997", "7210", "7211", "7216", "7230", "7251", "7295"};
	private String[] wholesale_mcc = {"5021", "5039", "5046", "5047", "5051", "5065", "5072", "5074", "5131", "5137", 
			"5172", "5193", "5998","4225", "4458", "4468", "5013", "5044", "5045", "5111", "5122", "5139", "5192", 
			"5198", "5398"};

	public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException{
		
		String[] keyTokens = key.toString().split(Constant.separator_1);
		String card_num = keyTokens[0].toString();
		String card_md5 = MD5.GetMD5Code(card_num);						//MD5
		String platinumCardFlg = "0";									// 白金卡卡号标识
		int cs_trx_unit_L6M = 0;										//广义消费笔数
		double  cs_trx_amt_L6M = 0d;								    //广义消费金额
		int T_No_Trx_M_L6M = 0;											//有广义消费的月份数目
		String month_record = "";										//有消费的月份记录
		String avg_amt_L6M = "";										//月均消费金额
		int merchant_count = 0;                                         //商户类别数
		
		
		//=============================== 计算开始 ========================================
		StringBuilder sb = new StringBuilder();
		ArrayList<TransItem> list = new ArrayList<TransItem>();
		
		
		int count = 0;
		for(Text line: lines){
			String[] tokens = line.toString().split(Constant.separator_1);
			//判断白金卡
			if (tokens.length == 1) {
				platinumCardFlg = "1";
			}else{
				count++;
				if(count>2000)
					break;
				
				TransItem item = new TransItem(line.toString());
				list.add(item);
				
				
			}
		}
		//=================计算广义消费金额 、广义消费笔数、消费月份数、商户类别数、月均 消费笔数==================
		
		if(list.size()>0){
			HashSet set = new HashSet();//有消费的月份集合
			HashSet merchantSet = new HashSet();//商户类别数
			StringBuilder monthSb = new StringBuilder();
			HashMap<String, Double> month_amt_map = new HashMap<String, Double>();
			
			for(TransItem item: list){
				if(this.isNormalTransId(item.getTrans_id()))
				{
					cs_trx_unit_L6M++;	//正常消费笔数加一
					if(!item.getFwd_ins_id_cd().equals("00010344"))//当接收机构代码为"00010344"时，视为境外银联卡交易
					{
						cs_trx_amt_L6M += item.trans_at;//正常消费金额增加
						
						//将每个月的广义消费进行累加
						if(month_amt_map.get(item.getMonth())!=null){
							double  value = (double )month_amt_map.get(item.getMonth());
							value += item.trans_at;
							month_amt_map.put(item.getMonth(), value);
						}
						else
							month_amt_map.put(item.getMonth(), item.trans_at);
					}
					else
					{
						cs_trx_amt_L6M += item.rcv_settle_at;
						
						//将每个月的广义消费进行累加
						if(month_amt_map.get(item.getMonth())!=null){
							double  value = (double )month_amt_map.get(item.getMonth());
							value += item.rcv_settle_at;
							month_amt_map.put(item.getMonth(), value);
						}
						else
							month_amt_map.put(item.getMonth(), item.rcv_settle_at);
					}
				}
				else if(this.isAbnormalTransId(item.getTrans_id())){
					cs_trx_unit_L6M--;
					if(!item.getFwd_ins_id_cd().equals("00010344"))
					{
						cs_trx_amt_L6M -= item.trans_at;
						
						//将每个月的广义消费进行累加
						if(month_amt_map.get(item.getMonth())!=null){
							double  value = (double )month_amt_map.get(item.getMonth());
							value -= item.trans_at;
							month_amt_map.put(item.getMonth(), value);
						}
						else
							month_amt_map.put(item.getMonth(), 0d);
					}
					else
					{
						cs_trx_amt_L6M -= item.rcv_settle_at;
						
						//将每个月的广义消费进行累加
						if(month_amt_map.get(item.getMonth())!=null){
							double  value = (double )month_amt_map.get(item.getMonth());
							value -= item.rcv_settle_at;
							month_amt_map.put(item.getMonth(), value);
						}
						else
							month_amt_map.put(item.getMonth(), 0d);
					}
				}
				
				set.add(item.getMonth());
				merchantSet.add(item.getMcc());
			}
			
			T_No_Trx_M_L6M = set.size();								//有消费月份数
			merchant_count = merchantSet.size();						//商户类别数
			
			Iterator it = set.iterator();
			while(it.hasNext()){
				monthSb.append((String)it.next()+Constant.separator_2);
			}
			if(monthSb.length()>Constant.separator_2.length())
				monthSb.setLength(monthSb.length()-Constant.separator_2.length());
			month_record = monthSb.toString();							//有消费月份记录
			
			
			ArrayList<String> monthKeyList = new ArrayList<String>();
			ArrayList<Double> monthAmntList = new ArrayList<Double>();
			Iterator month_it = month_amt_map.keySet().iterator();
			while(month_it.hasNext())
				monthKeyList.add((String)month_it.next());
				Collections.sort(monthKeyList);
				for(int i=1; i<monthKeyList.size(); i++){
					monthAmntList.add(month_amt_map.get(monthKeyList.get(i)));
				}
				removeNagetive(monthAmntList);					//去除0和负值
			
			avg_amt_L6M = this.getAverageAmount(monthAmntList);						//计算近6个月月均消费金额
			
		}
		
		
		
		//================= end 计算广义消费金额 、广义消费笔数==================
		
		sb.append(card_num+Constant.separator_1); 						//[0]卡号
		sb.append(cs_trx_unit_L6M+Constant.separator_1); 				//[1]消费总笔数
		sb.append(cs_trx_amt_L6M+Constant.separator_1); 				//[2]消费总金额
		sb.append(T_No_Trx_M_L6M+Constant.separator_1);					//[3]有消费的月份数 
		sb.append(avg_amt_L6M+Constant.separator_1);					//[4]月均消费金额
		sb.append(merchant_count+Constant.separator_1);					//[5]商户类别数
		
		context.write(new Text(sb.toString()), new Text(""));
	}
	
	class TransItem{
		
		private String cardNum;						//卡号
		private String md5;							//MD5
		private String iss_ins_code;				//发卡机构代码
		private String mcc;							//mcc
		private String mccName;						//mcc名称
		private String mchntName;					//商户名
		private String time;						//消费时间
		private String year;						//年
		private String month;						//月份
		private double  trans_at = 0;						//消费金额
		private String acptIns;						//受理机构
		private String locationCd;					//地区代码
		private String city;						//城市
		private String province;					//省份
		private String realProvince;				//真实省份
		
		private double  rcv_settle_at;				//清算金额
		private String card_bin;					//卡bin
		private String trans_id;					//交易类型
		private String fwd_ins_id_cd;				//发送机构标识码
	
		public TransItem(String cardNum, String md5, String mcc, String mccName,
				String mchntName, String time, double  trans_at, String acptIns) {
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
			this.iss_ins_code = tokens[6].trim();
			this.mcc = tokens[11].trim();
			this.mccName = tokens[18].trim();
			this.mchntName = tokens[13].trim();
			this.time = tokens[8].trim().substring(0,14);
			
			//System.out.println("=========:"+time);
			//this.year = time.substring(0,4);
			this.year = "";
			this.month = time.substring(0,6);                           // 例：201501 ++++++++++++++
			
			if(!tokens[15].trim().equals("") || !tokens[15].trim().equals(null))
				this.trans_at = Double.parseDouble (tokens[15].trim())/100;
			this.acptIns = tokens[7].trim();
			this.locationCd = this.acptIns.substring(4,8);				//地区代码
			this.city = Constant.getCity(this.locationCd);
			this.province = Constant.getProvince(this.city);
			this.realProvince = Constant.getCityKey(this.mchntName);
			
			if(!tokens[24].trim().equals("") || !tokens[24].trim().equals(null))
				this.rcv_settle_at = Double.parseDouble (tokens[24].trim())/100;
			this.card_bin = tokens[16].trim();
			this.trans_id = tokens[17].trim();
			this.fwd_ins_id_cd = tokens[22].trim();
			
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
		
		public String getIss_ins_code() {
			return iss_ins_code;
		}

		public void setIss_ins_code(String iss_ins_code) {
			this.iss_ins_code = iss_ins_code;
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
		
		public String getYear() {
			return year;
		}

		public void setYear(String year) {
			this.year = year;
		}

		public String getMonth() {
			return month;
		}

		public void setMonth(String month) {
			this.month = month;
		}

		public double  getTrans_at() {
			return trans_at;
		}

		public void setTrans_at(double  trans_at) {
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

		public double  getRcv_settle_at() {
			return rcv_settle_at;
		}

		public void setRcv_settle_at(double  rcv_settle_at) {
			this.rcv_settle_at = rcv_settle_at;
		}

		public String getCard_bin() {
			return card_bin;
		}

		public void setCard_bin(String card_bin) {
			this.card_bin = card_bin;
		}

		public String getTrans_id() {
			return trans_id;
		}

		public void setTrans_id(String trans_id) {
			this.trans_id = trans_id;
		}

		public String getFwd_ins_id_cd() {
			return fwd_ins_id_cd;
		}

		public void setFwd_ins_id_cd(String fwd_ins_id_cd) {
			this.fwd_ins_id_cd = fwd_ins_id_cd;
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
	
	/**
	 * 判断是否是普通交易类型（消费、预授权。。。）
	 * 
	 * */
	private boolean isNormalTransId(String trans_id){
		for(int i=0 ; i<normal_trans_id.length; i++){
			if(trans_id.equals(normal_trans_id[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 判断是否是非普通交易类型（冲正、撤销。。。）
	 * 
	 * */
	private boolean isAbnormalTransId(String trans_id){
		for(int i=0 ; i<abnormal_trans_id.length; i++){
			if(trans_id.equals(abnormal_trans_id[i]))
				return true;
		}
		return false;
	}
	
	/**
	 *  去除队列里的负值
	 * */
	private void removeNagetive(ArrayList<Double> list){
		for(int i=0; i<list.size(); i++){
			if(list.get(i)<=0)
				list.remove(i);
		}
	}
	
	/**
	 * 计算标准差
	 * 
	 * */
	private String getStandardDevition(ArrayList<Double> list){
		DecimalFormat df = new DecimalFormat("######0.0000"); 
		if(list.size()>0)
		{
			double  sum = 0f;
			
			for(double  elem : list)
				sum += elem;
			double  mean = sum/list.size();				//均值
			
			sum = 0f;
			for(double elem : list)
			{
				sum += (elem-mean)*(elem-mean);
			}
			
			return df.format(100*Math.sqrt(sum/list.size()));
		}
		else
			return df.format(0);
	}
	
	/**
	 * 计算授信额度（月均交易额）
	 * 
	 * */
	private String getAverageAmount(ArrayList<Double> list)
	{
		DecimalFormat df = new DecimalFormat("######0.00"); 
		double total = 0;
		double avg = 0;
		
		if(list.size()>0){
			for(double elem : list){
				total += elem;
			}
			
			avg = total/list.size();
			return df.format(avg);
		}
		else
			return df.format(0);
			
	}
}
