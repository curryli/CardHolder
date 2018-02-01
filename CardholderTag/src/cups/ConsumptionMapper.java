package cups;

/**
 * 从原始交易数据表里取
 * */

import java.io.IOException;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.up.util.Constant;
import com.up.util.MD5;
import com.up.util.TagUtility;

public class ConsumptionMapper extends Mapper<Object, Text, Text, Text>{
	//private final static String cardbinPath = "hdfs://ha-dev-nn:8020/user/hddtmn/association_model/card_bin";
	private final static Calendar calendar = Calendar.getInstance();
	private Hashtable<String, String> joinData = new Hashtable<String, String>();
	String priAcctNoConv = "-1";				//卡号
			
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
				
		//String target = context.getConfiguration().get("cardClass");
		String[] tokens = value.toString().split(",");
		
		if (tokens.length == 1) {
			if (tokens[0] == null || tokens[0].equals("")) {
				System.out.println("card num is null!");
				priAcctNoConv = "-1";
			} else {
				priAcctNoConv = tokens[0].trim();
			}
			
			context.write(new Text(priAcctNoConv), new Text(priAcctNoConv));
		} else
		{
			// 对字段总长度做检查
			if (tokens.length < 195) {
				System.out.println("tokens.length is illegal!");
				return;
			}
			if(tokens[37].replaceAll("\"", "").trim().equals(""))          //卡号为空
				return;
			else{
				for(int i = 0; i < tokens.length; i++)
					tokens[i] = tokens[i].replaceAll("\"", "").trim();
						
				//应答码00表示成功交易，剔除不成功交易
//				String resp = tokens[43].replaceAll("\"", "").trim();
//				if(!resp.equals("00"))
//					return;
						
				StringBuilder sb = new StringBuilder();
				String temp = "";
						
				//0.卡号
				String cardId = tokens[36].replaceAll("\"", "").trim();
				sb.append(cardId+",");
						
				//1.MD5
				String md5 = MD5.GetMD5Code(cardId);
				sb.append(md5+",");
						
				//2.卡种
				temp = tokens[19].replaceAll("\"", "").trim();
				sb.append(temp+",");
						
				//3.卡性质
				temp = tokens[20].replaceAll("\"", "").trim();
				sb.append(temp+",");
						
				//4.卡品牌          
				temp = tokens[53].replaceAll("\"", "").trim();
				sb.append(temp+",");
						
				//5.卡等级                           ++++++++++++++++++++++++++++++++++++++++++++++++++
				temp = tokens[53].replaceAll("\"", "").trim();
				sb.append(temp+",");
						
				//6.发卡机构代码
				String iss_ins_cd = tokens[29].replaceAll("\"", "").trim();
				sb.append(iss_ins_cd+",");
						
				//7.受理机构代码
				temp = tokens[33].replaceAll("\"", "").trim();
				//if(!Constant.isTargetAcptIns(temp))						//只筛选出目标受理地区
					//return;
				if(temp.length()<8)
					return;
				sb.append(temp+",");
						
						
				//8.时间
				//String to_ts = tokens[122].replaceAll("\"| |:|\\.|-", "");
				String to_ts = tokens[122].replaceAll("\"| |:|\\.|-", "");
				System.out.println("to_ts is :" + to_ts);
//				if ((!tokens[122].trim().matches("^-?\\d+$"))) {
//					System.out.println("to_ts is illegal，not all numerous! :" + to_ts);
//					return;
//				} 
				sb.append(to_ts+",");
				if(to_ts.length()<14)
					return;
						
				//9.年
				String year = to_ts.substring(0,4);
				sb.append(year+",");
						
				//10.月份
				//String hour = to_ts.substring(8,2);
				String month = to_ts.substring(4, 6);
				sb.append(month+",");
						
				//11.商户类型mcc
				String mcc = tokens[84].replaceAll("\"", "").trim();
				sb.append(mcc+",");
						
				//12.商户号                                      ++++++++++++++++++++++++++++++++++++++++++++++++++
				temp = tokens[92].replaceAll("\"", "").trim();
				sb.append(temp+",");
						
				//13.商户名
				String merchantName = tokens[93].replaceAll("\"", "").trim();
				sb.append(merchantName+",");
							
				//14.终端号
				temp = tokens[52].replaceAll("\"", "").trim();
				sb.append(temp+",");
						
				//15.交易金额
				String trans_at = tokens[62].replaceAll("\"", "").trim();
				System.out.println("trans_at is :" + trans_at);
				if (!(trans_at.matches("^\\d+.\\d+$") || trans_at.matches("^\\d+$"))) {
					System.out.println("trans_at is illegal，not all numerous!" + trans_at);
					trans_at = "0";
				} 
				sb.append(trans_at+",");
						
				//16.卡bin
				String card_bin = tokens[76].replaceAll("\"", "").trim();
				sb.append(card_bin+",");
						
				//17.交易类型
				String trans_id = tokens[48].replaceAll("\"", "").trim();
				sb.append(trans_id+",");
						
				//18.mcc名称
				String mcc_name = Constant.getMccName(mcc);
				sb.append(mcc_name+",");
						
				//19.mcc标签类型
				String mcc_type = Constant.getMccType(mcc);
				sb.append(mcc_type+",");
				
				//20.sti_takeout_in 交易被清算
				String sti_takeout_in = tokens[47].replace("\"", "").trim();
				sb.append(sti_takeout_in+",");
				
				//21.CUPS交易状态
				String cu_trans_st = tokens[46].replace("\"", "").trim();
				sb.append(cu_trans_st+",");
				
				//22.发送机构标识码
				String fwd_ins_id_cd = tokens[25].replace("\"", "").trim();
				sb.append(fwd_ins_id_cd+",");
				
				//23.清算发送机构标识码
				String settle_fwd_ins_id_cd  = tokens[60].replace("\"", "").trim();
				sb.append(settle_fwd_ins_id_cd+",");
				
				//24.清算金额                        检查清算金额 tokens[134] 全数字
				String rcv_settle_at = tokens[134].replaceAll("\"", "").trim();
				System.out.println("rcv_settle_at is :" + rcv_settle_at);
				if (!(tokens[134].trim().matches("^\\d+.\\d+$") || tokens[134].trim()
						.matches("^\\d+$"))) {
					System.out.println("rcv_settle_at is illegal，not all numerous!" + rcv_settle_at);
					rcv_settle_at = "0";
				} 
				sb.append(rcv_settle_at);
				
				/*
				//过滤加油类套码违规的商户交易
				if(mcc.equals("5541") || mcc.equals("5542")){
					if(!TagUtility.isNormalGasStationMchnt(merchantName))
						return;
				}
				
				//过滤停车场类套码违规的商户交易
				if(mcc.equals("7523")){
					if(!TagUtility.isNormalParkingMchnt(merchantName))
						return;
				}
				
				//过滤路桥通行费类套码违规的商户交易
				if(mcc.equals("4784")){
					if(!TagUtility.isNormalTollMchnt(merchantName))
						return;
				}
				
				*/
				
				//交易没被清算，跳过
				if(!sti_takeout_in.equals("1"))
					return;
				
				//交易被冲正，跳过
				int trans_st = getTrans_st(cu_trans_st, trans_id);
				if(trans_st==3)
					return;

						
				//key: 卡号+发卡机构代码
//				if(TagUtility.CAR_MCC_SET.contains(mcc))
//					context.write(new Text(cardId+Constant.separator_1
//							+iss_ins_cd+Constant.separator_1
//							+card_bin
//							), new Text(sb.toString()));
				context.write(new Text(cardId), new Text(sb.toString()));
			}
		}	
		
		
	}

	//计算交易是否被冲正， trans_st = 3 交易未冲正
	public int getTrans_st(String cu_trans_st, String trans_id){
		int trans_st = 0;
		
		if(cu_trans_st == null || cu_trans_st.equals("") || cu_trans_st.length() < 5) {
			trans_st = 0;
		}
		else{
			if(cu_trans_st.equals("10000"))
				trans_st = 1;
			else if(cu_trans_st.substring(1,2).equals("1"))
				trans_st = 2;
			else if(cu_trans_st.substring(3,4).equals("1")){
				if(trans_id.equals("S33") || trans_id.equals("S32"))
					trans_st = 4;
				else
					trans_st = 3;
			}
			else 
				trans_st =5;
		}
		return trans_st;
		
	}
}
