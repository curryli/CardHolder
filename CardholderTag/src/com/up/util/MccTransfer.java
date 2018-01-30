package com.up.util;

import java.util.Hashtable;

public class MccTransfer {
	public enum MccTag{
		retailTag("零售","01"), groceryTag("食品","02"), clothingTag("成人服饰","03"), restaurant("餐饮","04"), house("家居装修","05");
		
		private String categoryName;
		private String transferTag;
		
		private MccTag(String name , String tag){
			this.categoryName = name;
			this.transferTag = tag;
		}
		
		public String getTransferTag(){
			return this.transferTag;
		}
	}
	
	public static String[] RETAIL = {"5311","5411","5331","5399"};             //零售
	public static String[] GROCERY = {"5441","5451","5462","5499"};            //食品 
	public static String[] CLOTHING = {"5611","5651","5655","5699","5661","5681","5691","5697"};       //服饰类
	public static String[] RESTAURANT ={"5812","5814"};                        //餐饮
	//public static String[] HOUSE={"5211","","","","","",""};                        //家居装修
	public static Hashtable<String,String> table;
	public static Hashtable<String,String> mccNameTable;
	public static Hashtable<String,String> timeTable;
	
	static{
		table = new Hashtable();
		mccNameTable = new Hashtable();
		timeTable = new Hashtable();
		
		for(int i = 0 ; i < RETAIL.length ; i++){
			table.put(RETAIL[i], MccTag.retailTag.getTransferTag());
		}
		/*
		for(int i = 0 ; i < GROCERY.length ; i++){
			table.put(GROCERY[i], MccTag.groceryTag.getTransferTag());
		}*/
		for(int i = 0 ; i < CLOTHING.length ; i++){
			table.put(CLOTHING[i], MccTag.clothingTag.getTransferTag());
		}
		for(int i = 0 ; i < RESTAURANT.length ; i++){
			table.put(RESTAURANT[i], MccTag.restaurant.getTransferTag());
		}
		
		mccNameTable.put("01", "零售");
		mccNameTable.put("02", "食品");
		mccNameTable.put("03", "成人服饰");
		mccNameTable.put("04", "餐饮");
		mccNameTable.put("5441", "糖果及坚果商店 ");
		mccNameTable.put("5451", "乳制品店、冷饮店");
		mccNameTable.put("5462", "面包房、糕点商店 ");
		mccNameTable.put("5499", "各类食品店及专门食品零售店");
		mccNameTable.put("3333", "网络公司航空保险旅行社在线支付");
		mccNameTable.put("5422", "肉类零售");
		mccNameTable.put("5921", "瓶装酒零售店 ");
		mccNameTable.put("5993", "香烟雪茄专卖店");
		mccNameTable.put("5621", "妇女成衣商店 ");
		mccNameTable.put("5631", "女性用品商店 ");                                 //-----------------------
		mccNameTable.put("5641", "婴儿儿童服装店 ");
		mccNameTable.put("5698", "假发商店 ");
		mccNameTable.put("5947", "礼品卡片装饰品纪念品商店");
		mccNameTable.put("5948", "箱包皮具店 ");
		mccNameTable.put("5972", "邮票和纪念币商店 ");
		mccNameTable.put("5973", "宗教品商店");
		mccNameTable.put("5975", "助听器－销售、服务和用品");
		mccNameTable.put("5977", "化妆品商店");
		mccNameTable.put("5995", "宠物商店、宠物食品及用品");
		mccNameTable.put("8043", "光学产品、眼镜店");
		mccNameTable.put("5094", "贵重珠宝、首饰，钟表零售");
		mccNameTable.put("5944", "银器店");
		mccNameTable.put("5950", "玻璃器皿和水晶饰品店");
		mccNameTable.put("5970", "工艺美术商店 ");
		mccNameTable.put("5971", "艺术商和画廊 ");
		mccNameTable.put("5949", "纺织品及针织品零售 ");
		mccNameTable.put("5733", "音乐商店乐器");
		mccNameTable.put("5735", "音像制品商店 ");
		mccNameTable.put("5932", "古玩店出售维修及还原");
		mccNameTable.put("5937", "古玩复制店");
		mccNameTable.put("5942", "书店");
		mccNameTable.put("5943", "文具用品商店、各类办公用品商店");
		mccNameTable.put("5945", "玩具游戏店 ");                                                  //----------------
		mccNameTable.put("5946", "照相器材商店 ");                                                //----------------------
		mccNameTable.put("5994", "电子游戏供给");
		mccNameTable.put("5940", "自行车商店");
		mccNameTable.put("5941", "体育用品店");
		mccNameTable.put("5996", "游泳池");
		mccNameTable.put("5912", "药房药店 ");
		mccNameTable.put("5976", "整形外科） ");
		mccNameTable.put("5271", "活动房车销售商");
		mccNameTable.put("5511", "汽车货车经销商－新旧车的销售");
		mccNameTable.put("5521", "汽车货车经销商－专门从事旧车");
		mccNameTable.put("5532", "汽车轮胎经销商 ");
		mccNameTable.put("5533", "汽车零配件商店 ");
		mccNameTable.put("5541", "加油站服务站 ");
		mccNameTable.put("5542", "自助加油站 ");
		mccNameTable.put("5551", "船只经销商 ");
		mccNameTable.put("5561", "旅行拖车、娱乐用车销售商");
		mccNameTable.put("5571", "摩托车商店和经销商 ");
		mccNameTable.put("5592", "露营房车销售商 ");
		mccNameTable.put("5598", "雪车商 ");
		mccNameTable.put("5599", "汽车飞行器农用机车综合经营商");
		mccNameTable.put("5983", "燃料经销商");
		mccNameTable.put("4812", "通讯设备和电话销售（商） ");
		mccNameTable.put("5722", "家用电器商店 ");
		mccNameTable.put("5732", "电子设备商店 ");
		mccNameTable.put("5734", "计算机软件商店 ");
		mccNameTable.put("5978", "打字机商店－销售服务和出租");
		mccNameTable.put("5997", "电动剃刀商店－销售和服务");
		mccNameTable.put("5211", "木材和各类建材卖场");
		mccNameTable.put("5231", "玻璃油漆涂料墙纸零售");
		mccNameTable.put("5251", "五金商店 ");
		mccNameTable.put("5992", "花店");
		mccNameTable.put("5261", "草坪花园用品商店 ");
		mccNameTable.put("5712", "家具家庭摆品家用设备零售商");
		mccNameTable.put("5713", "地板商店 ");
		mccNameTable.put("5714", "装潢商店");
		mccNameTable.put("5718", "壁炉壁炉防护网及配件商店");
		mccNameTable.put("5719", "各种家庭装饰专营店 ");
		mccNameTable.put("5200", "大型家具卖场（暂时停用） ");
		mccNameTable.put("5300", "会员制批量零售店（暂时停用） ");
		mccNameTable.put("5309", "免税商店");
		mccNameTable.put("5310", "折扣商店");
		mccNameTable.put("5931", "二手商品店");
		mccNameTable.put("5999", "其他专门零售店");
		mccNameTable.put("7011", "住宿服务旅馆酒店 ");
		mccNameTable.put("7012", "分时使用的别墅或度假用房");
		mccNameTable.put("5812", "就餐场所和餐馆 ");
		mccNameTable.put("5813", "饮酒场所（酒吧酒馆夜总会鸡尾酒大厅） ");
		mccNameTable.put("5814", "便民餐饮店 ");
		mccNameTable.put("7032", "运动和娱乐露营地 ");
		mccNameTable.put("7033", "活动房车场及露营场所");
		mccNameTable.put("7297", "按摩店 ");
		mccNameTable.put("7298", "保健及美容SPA ");
		mccNameTable.put("7829", "电影和录像创作发行");
		mccNameTable.put("7832", "电影院 ");
		mccNameTable.put("7911", "歌舞厅 ");
		mccNameTable.put("7922", "戏剧制片演出和票务");
		mccNameTable.put("7929", "未列入其他代码的乐队、文艺表演");
		mccNameTable.put("7932", "台球 ");
		mccNameTable.put("7933", "保龄球馆 ");
		mccNameTable.put("7941", "商业体育场馆、职业体育俱乐部、运动场和体育推广公司");
		mccNameTable.put("7991", "旅游与展览 ");
		mccNameTable.put("4733", "大型景区售票 ");
		mccNameTable.put("7992", "公共高尔夫球场");
		mccNameTable.put("7994", "大型游戏机和游戏场所");
		mccNameTable.put("7995", "彩票销售 ");
		mccNameTable.put("7996", "游乐园马戏团嘉年华占卜");
		mccNameTable.put("7997", "会员俱乐部（体育娱乐运动等）");
		mccNameTable.put("7998", "水族馆海洋馆和海豚馆");
		mccNameTable.put("7999", "未列入其他代码的娱乐服务");
		mccNameTable.put("1520", "一般承包商－住宅与商业楼");
		mccNameTable.put("7013", "不动产代理－房地产经纪");
		mccNameTable.put("7299", "未列入其他代码的个人服务（其他房地产服务） ");
		mccNameTable.put("5933", "当铺 ");
		mccNameTable.put("6051", "非金融机构－外币兑换非电子转帐的汇票");
		mccNameTable.put("6211", "证券公司－经纪人和经销商");
		mccNameTable.put("6300", "保险销售");
		mccNameTable.put("6010", "金融机构－人工现金支付");
		mccNameTable.put("6011", "金融机构－自动现金支付");
		mccNameTable.put("6012", "金融机构－商品和服务");
		mccNameTable.put("9498", "信用卡还款 ");
		mccNameTable.put("4900", "公共事业（水电煤服务） ");
		mccNameTable.put("7210", "洗衣店 ");
		mccNameTable.put("7211", "洗熨服务（自助洗衣服务） ");
		mccNameTable.put("7216", "干洗店 ");
		mccNameTable.put("7217", "室内清洁服务（地毯沙发家具表面的清洁服务）");
		mccNameTable.put("7221", "摄影工作室");
		mccNameTable.put("7230", "理发店");
		mccNameTable.put("7261", "殡葬服务");
		mccNameTable.put("7273", "婚姻介绍及陪同服务");
		mccNameTable.put("7295", "家政服务");
		mccNameTable.put("7395", "照相洗印服务 ");
		mccNameTable.put("7523", "停车场 ");
		mccNameTable.put("7299", "未列入其他代码的其他个人服务");
		mccNameTable.put("0763", "农业合作 ");
		mccNameTable.put("0780", "景观美化及园艺服务");
		mccNameTable.put("4722", "旅行社");
		mccNameTable.put("5811", "包办伙食宴会承包商");
		mccNameTable.put("5935", "海上船只遇难救助");
		mccNameTable.put("7276", "税收准备服务");
		mccNameTable.put("7277", "咨询服务－债务婚姻和私人事务");
		mccNameTable.put("7278", "购物服务及会所（贸易经纪服务） ");
		mccNameTable.put("7311", "广告服务 ");
		mccNameTable.put("7321", "消费者信用报告机构 ");
		mccNameTable.put("7333", "商业摄影、工艺、绘图服务");
		mccNameTable.put("7338", "复印及绘图服务");
		mccNameTable.put("7339", "速记秘书服务（包括各类办公服务） ");
		mccNameTable.put("7361", "职业中介临时工 ");
		mccNameTable.put("7392", "管理咨询和公共关系服务");
		mccNameTable.put("7393", "侦探保安安全服务");
		mccNameTable.put("7549", "拖车服务");
		mccNameTable.put("8111", "法律服务和律师事务所服务");
		mccNameTable.put("8675", "汽车协会 ");
		mccNameTable.put("8931", "会计审计财务服务");
		mccNameTable.put("7399", "未列入其他代码的商业服务");
		mccNameTable.put("8911", "建筑工程和测量服务");
		mccNameTable.put("8912", "装修 ");
		mccNameTable.put("4457", "出租船只 ");
		mccNameTable.put("4468", "船舶海运服务提供商");
		mccNameTable.put("7296", "出租衣物－服装制服和正式场合服装");
		mccNameTable.put("7394", "设备工具家具和电器出租");
		mccNameTable.put("7512", "汽车出租 ");
		mccNameTable.put("7513", "卡车及拖车出租");
		mccNameTable.put("7519", "房车和娱乐车辆出租 ");
		mccNameTable.put("7841", "音像制品出租商店 ");
		mccNameTable.put("3998", "中华人民共和国铁道部");
		mccNameTable.put("4111", "本市和市郊通勤旅客运输包括轮渡");
		mccNameTable.put("4112", "铁路客运");
		mccNameTable.put("4119", "救护车服务 ");
		mccNameTable.put("4121", "出租车服务 ");
		mccNameTable.put("4131", "公路客运 ");
		mccNameTable.put("4411", "轮船及巡游航线服务 ");
		mccNameTable.put("4511", "航空公司 ");
		mccNameTable.put("4582", "机场服务 ");
		mccNameTable.put("4784", "路桥通行费");
		mccNameTable.put("4789", "未列入其他代码的运输服务");
		mccNameTable.put("4011", "铁路运输 ");
		mccNameTable.put("4214", "货物搬运和托运");
		mccNameTable.put("4215", "快递服务（空运地面运输或海运）");
		mccNameTable.put("4225", "公共仓储服务－农产品");
		mccNameTable.put("9402", "国家邮政服务 ");
		mccNameTable.put("4814", "电信服务");
		mccNameTable.put("4816", "计算机网络信息服务");
		mccNameTable.put("4821", "电报服务 ");
		mccNameTable.put("7372", "计算机编程数据处理和系统集成设计服务");
		mccNameTable.put("7375", "信息检索服务 ");
		mccNameTable.put("4899", "有线和其他付费电视服务");
		mccNameTable.put("0742", "兽医服务");
		mccNameTable.put("6513", "不动产管理－物业管理");
		mccNameTable.put("7251", "修鞋店擦鞋店帽子清洗店");
		mccNameTable.put("7342", "灭虫及消毒服务 ");
		mccNameTable.put("7349", "清洁保养及门卫服务");
		mccNameTable.put("7379", "未列入其他代码的计算机维护和修理服务");
		mccNameTable.put("7531", "车体维修店 ");
		mccNameTable.put("7534", "轮胎翻新、维修店 ");
		mccNameTable.put("7535", "汽车喷漆店");
		mccNameTable.put("7538", "汽车服务商店非经销商 ");
		mccNameTable.put("7542", "洗车 ");
		mccNameTable.put("7622", "电器设备维修 ");
		mccNameTable.put("7623", "空调制冷设备维修 ");
		mccNameTable.put("7629", "电器设备小家电维修");
		mccNameTable.put("7631", "手表首饰维修店");
		mccNameTable.put("7641", "家具维修");
		mccNameTable.put("7692", "焊接维修服务 ");
		mccNameTable.put("7699", "各类维修店及相关服务");
		mccNameTable.put("7993", "电子游戏供给");
		mccNameTable.put("8999", "未列入其他代码的专业服务");
		mccNameTable.put("8211", "中小学校（公立）");
		mccNameTable.put("8220", "普通高校（公立） ");
		mccNameTable.put("8241", "函授学校（成人教育） ");
		mccNameTable.put("8244", "商业和文秘学校（中等专业学校）");
		mccNameTable.put("8249", "贸易和职业学校（职业技能培训）");
		mccNameTable.put("8299", "其他学校和教育服务");
		mccNameTable.put("8351", "儿童保育服务（含学前教育） ");
		mccNameTable.put("8011", "其他医疗卫生活动");
		mccNameTable.put("8021", "牙科医生");
		mccNameTable.put("8031", "正骨医生");
		mccNameTable.put("8041", "按摩医生");
		mccNameTable.put("8042", "眼科医生 ");
		mccNameTable.put("8049", "手足病医生 ");
		mccNameTable.put("8050", "护理和照料服务 ");
		mccNameTable.put("8062", "公立医院");
		mccNameTable.put("8071", "医学及牙科实验室");
		mccNameTable.put("8099", "其他医疗保健服务");
		mccNameTable.put("8399", "公共资源交易中心");                        //-------------------------------
		mccNameTable.put("8641", "市民、社会及友爱组织");
		mccNameTable.put("8651", "政治组织（政府机构）");
		mccNameTable.put("8661", "宗教组织 ");
		mccNameTable.put("8699", "其他会员组织");
		mccNameTable.put("8398", "慈善和社会公益服务组织");
		mccNameTable.put("9211", "法庭费用");
		mccNameTable.put("9222", "罚款");
		mccNameTable.put("9223", "保释金");
		mccNameTable.put("9311", "纳税");
		mccNameTable.put("9400", "使领馆收费");
		mccNameTable.put("9399", "未列入其他代码的政府服务");
		mccNameTable.put("5013", "机动车零配件批发商 ");
		mccNameTable.put("5021", "办公及商务家具批发商");
		mccNameTable.put("5039", "未列入其他代码的建材批发批发商");
		mccNameTable.put("5044", "办公影印摄影器材批发商");
		mccNameTable.put("5045", "计算机设备批发商");
		mccNameTable.put("5046", "未列入其他代码的商用器材（批发商）");
		mccNameTable.put("5047", "医院器材批发商 ");
		mccNameTable.put("5051", "金属产品批发商");
		mccNameTable.put("5065", "电器零件批发商");
		mccNameTable.put("5072", "五金器材批发商");
		mccNameTable.put("5074", "管道和供暖设备批发商 ");
		mccNameTable.put("5111", "文具办公用品批发商");
		mccNameTable.put("5122", "药品批发商");
		mccNameTable.put("5131", "布料纺织品批发商");
		mccNameTable.put("5137", "男女及儿童制服和服装批发商 ");
		mccNameTable.put("5139", "鞋类批发商");
		mccNameTable.put("5172", "石油产品批发商");
		mccNameTable.put("5192", "期刊和报纸批发");
		mccNameTable.put("5193", "花卉批发商");
		mccNameTable.put("5198", "油漆批发商");
		mccNameTable.put("5398", "大型企业批发");
		mccNameTable.put("4458", "烟草配送商户");
		mccNameTable.put("5998", "其他批发商");
		mccNameTable.put("5960", "保险直销");
		mccNameTable.put("5962", "旅游相关服务直销");
		mccNameTable.put("5963", "门对门销售");
		mccNameTable.put("5964", "目录直销商户");
		mccNameTable.put("5965", "目录、零售兼营商户");
		mccNameTable.put("5966", "电话呼出直销");
		mccNameTable.put("5967", "电话呼入直销");
		mccNameTable.put("5968", "订阅/订购直销服务");
		mccNameTable.put("5969", "其他直销商户");
		
		timeTable.put("00", "0000");                                    //第一时间段00-05
		timeTable.put("01", "0001");
		timeTable.put("02", "0204");
		timeTable.put("03", "0204");
		timeTable.put("04", "0204");
		timeTable.put("05", "0005");
		timeTable.put("06", "0006");                                     //第二时间段06-08
		timeTable.put("07", "0007");
		timeTable.put("08", "0008");
		timeTable.put("09", "0009");                                     //第三时间段09-11
		timeTable.put("10", "0010");
		timeTable.put("11", "0011");
		timeTable.put("12", "0012");                                     //第四时间段12-14
		timeTable.put("13", "0013");
		timeTable.put("14", "0014");
		timeTable.put("15", "1516");                                      //第五时间段15、16
		timeTable.put("16", "1516");
		timeTable.put("17", "0017");                                       //第六时间段17-19
		timeTable.put("18", "0018");
		timeTable.put("19", "0019");
		timeTable.put("20", "2022");                                        //第七时间段20-23
		timeTable.put("21", "2022");
		timeTable.put("22", "2022");
		timeTable.put("23", "0023");
		
	}
	
	public static String transfer(String mcc){
		if(table.get(mcc)!=null)
			return table.get(mcc);
		else 
			return mcc;		
	}
	
	public static String getMccName(String num){
		if(mccNameTable.get(num)!= null)
			return mccNameTable.get(num);
		else 
			return num;
	}
	
	public static String getTimeTransfer(String time){
		if(timeTable.get(time)!=null)
			return timeTable.get(time);
		else
			return time;
	}

}
