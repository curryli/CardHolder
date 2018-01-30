package com.up.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class TagUtility {
	public static String NULL = "null";
	public static String HIGH_CONFIDENCE = "2";					//高置信度
	public static String LOW_CONFIDENCE = "1";					//低置信度
	public static float PURCHASE_CAR_THRESHOLD = 20000f;		//经销商处买车交易下限
	public static float LOW_REFUEL_THRESHOLD = 400f;			//低排量汽车加油上限
	public static float MEDIA_REFUEL_THRESHOLD = 800f;			//中排量汽车加油上限
	public static float FAKE_REFUEL_THRESHOLD = 3000f;			//虚假加油金额下限
	public static int[] TIME_INTERVAL = {0,6,9,12,13,17,24};	//各时间段边界
	
	public static final String FALSE_TYPE = "false";
	public static final String TEA_TYPE = "tea";
	public static final String BAR_TYPE = "bar";
	public static final String CAFE_TYPE = "cafe";
	public static final String KTV_TYPE = "ktv";
	public static final String MASSAGE_TYPE = "massage";
	public static final String CLUB_TYPE = "club";
	public static final String FOOD_TYPE = "food";
	
	public static final String FIT_TYPE = "fit";
	public static final String YOGA_TYPE = "yoga";
	public static final String GOLF_TYPE = "golf";
	public static final String TENNIS_TYPE = "tennis";
	public static final String BADMINTON_TYPE = "badminton";
	public static final String PINGPONG_TYPE = "pingpong";
	public static final String BOWLING_TYPE = "bowling";
	public static final String SNOKE_TYPE = "snoke";
	public static final String TAEKWONDO_TYPE = "taekwondon";
	public static final String SWORDPLAY_TYPE = "swordplay";
	public static final String HORSEMANSHIP_TYPE = "horsemanship";
	public static final String SWIM_TYPE = "swim";	
	public static final String SPORT_TYPE ="sport";
	
	//时间段名称
	public static String[] TIME_INTERVAL_NAME = {"ERROR","0-6","6-9","9-12","12-13","13-17","17-24"};
	
	public static int[] AMOUNT_INTERVAL = {0,200,300,400,500,600,700,800,1000};
	public static String[] AMOUNT_INTERVAL_NAME = {"ERROR","0-200","200-300","300-400","400-500","500-600","600-700","700-800","800-1000","1000+"};
	
	public static String[] WEEK_DAYS = {"SUM","MON","TUE","WED","THU","FRI","SAT"};			//星期名
	
	//=================汽车类关键字============================
	public static String[] REFUEL_KEY = {"加油","石油","汽油","柴油","石化","航油","化工","燃料","壳牌","中经汇通","油品","天然气",
										 "中油","燃气","油气","油料","道达尔","油","站","供热","服务","销售","股份",
										 "责任","延长","中化","中心","高速","润滑油","能源","中海油","中航","集团","交通"};
	public static String[] PARKING_KEY = {"停车","收费","管理","物业","车库","服务","汽车","公司","有限","责任","供应"};
	public static String[] TOLL_KEY = {"公路","高速","电子","收费","服务","营运","运营","车库","路网","中心","管理","开发","ETC","监控",
									   "养护","监测","责任","联网","交通"};
	public static String[] REFUEL_BRAND_KEY = {"壳牌","道达尔","BP","中经汇通","中石油","中油","中石化","中化","中海油","中国海洋","海洋石油"};	
	
	//=================娱乐夜生活类关键字============================
	public static String[] BAR_KEY = {"酒吧","酒廊","酒屋","酒城","酒行","酒都","酒巴","酒屋","酒业","红酒","BAR","Bar","bar","Beer","BEER",
										"beer","WINE","Wine","wine","PUB","pub","井阳岗"};
	public static String[] CAFE_KEY = {"咖","星巴克","啡","CAFé","CAFE","Cafe","CAFE","cafe","COFFEE","coffee","Coffee","COSTA","costa"};
	public static String[] KTV_KEY = {"ktv","KT","kTV","ＫＴＶ","ｋｔｖ","TKV","KTW","RTV","KYV","歌","唱","麦克风","麦霸","音","卡拉","纯K",
										"量贩","量版","贩","摇滚","爵士","ok","OK","JAZZ","米乐星"};
	public static String[] TEA_KEY = {"茶","香茗","壶","棋"};
	public static String[] MASSAGE_KEY = {"足","浴","桑","桑拿","保健","疗","温泉","养生","按摩","按","压","推","养身","推拿","医","汗",
										"蒸","脚","调理","脊","梳理","健康","康乐","massage","MASSAGE","Massage","SPA","spa","Spa"};
	public static String[] CLUB_KEY = {"会所","会馆","会","俱乐","夜总会","夜","国际","公馆","公会","休闲","馆","阁","湾","娱","中心","CLUB","Club",
										"club","辉煌","豪","度假","渡假","雅座","假日","人间","天上","巴黎","年华","盛世","海岸","荟",
										"庄","壹","宫","舞","皇","尊","帝","钻","莊","文化","外滩"};
	public static String[] FOOD_KEY = {"饭","餐","饮","食","菜","吃","烧","烤","味","海鲜","锅","甜","煲","粉","骨","饺子","米线","米粉",
										"酒楼","酒家","西点","冰","比萨","批萨","汉堡","辣","骨","糖","粥","料理","鱼","鹅","鸡","羊","渔",
										"汤","牛","鸭","饼","啤","豆","蛋糕","面包","面","肠","烟","奶","厨","农","仙踪林"};
	public static String[] FALSE_KEY1 = {"加油","电器","服装","服饰","家电","百货","茶餐厅","日用","美容","美发","干杂","电影","客栈","配送"};
	public static String[] FALSE_KEY2 = {"经营","HOTEL","Hotel","hotel","酒店","便利","招待","工作","代理","商行","超市","购物","科技","商务","商贸"};
	
	public static String[] FIT_KEY = {"健身","建身","健美","健体","健壮","FITNESS","fitness","Fitness","一兆韦德","威尔士"};
	public static String[] YOGA_KEY = {"瑜伽","瑜珈","舞","yoga","YOGA","Yoga"};
	public static String[] GOLF_KEY = {"高尔夫","golf","GOLF","Golf"};
	public static String[] TENNIS_KEY = {"网球","TENNIS"};
	public static String[] BADMINTON_KEY = {"羽毛","羽球","乒"};
	public static String[] TAEKWONDO_KEY = {"拳","武术"};
	public static String[] BOWLING_KEY = {"保龄"};
	public static String[] HORSEMANSHIP_KEY = {"马术"};
	public static String[] SWORDPLAY_KEY = {"击剑"};
	public static String[] SNOKE_KEY = {"台球","桌球"};
	public static String[] SWIM_KEY = {"游","泳","水上"};
	//public static String[] SPORT_KEY = {"网球","马术","击剑","跆拳道","保龄球","TENNIS","羽毛球","羽球"};
	public static String[] SPORT_FALSE1 = {"加油","服装","服饰","家电","干杂","电影","客栈","配送","夜总会"};
	public static String[] SPORT_FALSE2 = {"经营","HOTEL","Hotel","hotel","酒店","电器","便利","招待","美容","美发","理发","工作","超市","购物","科技","商务","商贸"};
	
	
	public static HashSet<String> CAR_MCC_SET;					//汽车类MCC
	public static HashSet<String> ENTTM_MCC_SET;				//娱乐夜生活类MCC
	public static HashSet<String> HIGH_CONF_SET;				//自有汽车高置信度MCC
	public static HashSet<String> MAINTAIN_MCC_SET;
	public static HashMap<String, String> REAUEL_BRAND_MAP;
	public static String[] car_mcc = {
		"5271",				//活动房车销售商
		"5511",				//汽车货车经销商-新旧车的销售、服务、维修、零件及出租
		"5521",				//汽车货车经销商-专门从事旧车的销售、服务、维修、零件及出租
		"5532",				//汽车轮胎经销商
		"5533",				//汽车零配件商店
		"5541",				//加油站、服务站
		"5542",				//自助加油站
		"7523",				//停车场
		"7512",				//汽车出租
		"7513",				//卡车及拖车出租
		"4784",				//路桥通行费
		"7531",				//车体维修店
		"7534",				//轮胎翻新维修店
		"7535",				//汽车喷漆店
		"7538",				//汽车服务商店
		"7542",				//洗车
		};
	public static String[] high_confidence_mcc = {
		"5271",				//活动房车销售商
		"5511",				//汽车货车经销商-新旧车的销售、服务、维修、零件及出租
		"5521",				//汽车货车经销商-专门从事旧车的销售、服务、维修、零件及出租	
		"5541",				//加油站、服务站
		"5542",				//自助加油站
		"7523",				//停车场
		"4784",				//路桥通行费
		"7531",				//车体维修店
		"7534",				//轮胎翻新维修店
		"7535",				//汽车喷漆店
		"7542",				//洗车
	};
	
	public static String[] maintain_mcc = {
		"5532",				//汽车轮胎经销商
		"5511",				//汽车货车经销商-新旧车的销售、服务、维修、零件及出租
		"5521",				//汽车货车经销商-专门从事旧车的销售、服务、维修、零件及出租	
		"5533",				//汽车零配件商店
		"7531",				//车体维修店
		"7534",				//轮胎翻新维修店
		"7535",				//汽车喷漆店
		"7538",				//汽车服务商店
		"7542",				//洗车
	};
	
	public static String[] entertainment_mcc = {
		"5813",				//饮酒场所（酒吧、酒馆、夜总会、鸡尾酒大厅、迪斯科舞厅）
		"7911",				//歌舞厅
		"7297",				//按摩店
		"7298",				//保健及美容SPA
		
		"7932",				//台球、撞球场所
		"7933",				//保龄球馆
		"7992",				//公共高尔夫球场
		"7997",				//会员俱乐部（体育、娱乐、运动等）、乡村俱乐部以及私人高
		"7903",				//运动和娱乐露营地（健身馆）
		"7941"				//商业体育场馆、职业体育俱乐部、运动场和体育推广公司
	};
	
	static{
		CAR_MCC_SET = new HashSet<String>();
		for(int i=0; i<car_mcc.length; i++){
			CAR_MCC_SET.add(car_mcc[i]);
		}
		
		ENTTM_MCC_SET = new HashSet<String>();
		for(int i=0; i<entertainment_mcc.length; i++){
			ENTTM_MCC_SET.add(entertainment_mcc[i]);
		}
		
		HIGH_CONF_SET = new HashSet<String>();
		for(int i=0; i<high_confidence_mcc.length; i++){
			HIGH_CONF_SET.add(high_confidence_mcc[i]);
		}
		
		MAINTAIN_MCC_SET = new HashSet<String>();
		for(int i=0; i<maintain_mcc.length; i++){
			MAINTAIN_MCC_SET.add(maintain_mcc[i]);
		}
		
		REAUEL_BRAND_MAP = new HashMap<String, String>();
		REAUEL_BRAND_MAP.put("中国石油天然气", "中石油");
		REAUEL_BRAND_MAP.put("中石油", "中石油");
		REAUEL_BRAND_MAP.put("中油", "中石油");
		REAUEL_BRAND_MAP.put("中国石油化工", "中石化");
		REAUEL_BRAND_MAP.put("中国石化", "中石化");
		REAUEL_BRAND_MAP.put("中石化", "中石化");
		REAUEL_BRAND_MAP.put("中化", "中石化");
		REAUEL_BRAND_MAP.put("壳牌", "壳牌");
		REAUEL_BRAND_MAP.put("中经汇通", "中经汇通");
		REAUEL_BRAND_MAP.put("道达尔", "道达尔");
		REAUEL_BRAND_MAP.put("中海油", "中海油");
		REAUEL_BRAND_MAP.put("海洋石油", "中海油");
		REAUEL_BRAND_MAP.put("中国海洋", "中海油");
		REAUEL_BRAND_MAP.put("BP", "英国石油BP");
	}
	
	/**
	 * 判断是否自驾类相关交易
	 * */
	public static boolean isDriveTrans(String mcc){
		if(mcc.equals("5541") ||			//加油站
		   mcc.equals("5542") ||			//自助加油站
		   mcc.equals("4784") ||			//路桥通行
		   mcc.equals("7523") ||			//停车场
		   mcc.equals("7512"))				//租车
			return true;
		else
			return false;
	}
	
	/**
	 * 根据关键字判断是否正规加油站。
	 * 加油类商户套码违规严重
	 * */
	public static boolean isNormalGasStationMchnt(String str){
		for(int i=0; i<REFUEL_KEY.length; i++){
			if(str.contains(REFUEL_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否停车场。
	 * 加油类商户套码违规严重
	 * */
	public static boolean isNormalParkingMchnt(String str){
		for(int i=0; i<PARKING_KEY.length; i++){
			if(str.contains(PARKING_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否正规收费站。
	 * 加油类商户套码违规严重
	 * */
	public static boolean isNormalTollMchnt(String str){
		for(int i=0; i<TOLL_KEY.length; i++){
			if(str.contains(TOLL_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于酒吧
	 * */
	public static boolean isBarMchnt(String str){
		for(int i=0; i<BAR_KEY.length; i++){
			if(str.contains(BAR_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于咖啡厅
	 * */
	public static boolean isCafeMchnt(String str){
		for(int i=0; i<CAFE_KEY.length; i++){
			if(str.contains(CAFE_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于KTV
	 * */
	public static boolean isKTVMchnt(String str){
		for(int i=0; i<KTV_KEY.length; i++){
			if(str.contains(KTV_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于茶馆
	 * */
	public static boolean isTeaMchnt(String str){
		for(int i=0; i<TEA_KEY.length; i++){
			if(str.contains(TEA_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于推倒洗浴
	 * */
	public static boolean isMassageMchnt(String str){
		for(int i=0; i<MASSAGE_KEY.length; i++){
			if(str.contains(MASSAGE_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于夜总会
	 * */
	public static boolean isClubMchnt(String str){
		for(int i=0; i<CLUB_KEY.length; i++){
			if(str.contains(CLUB_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于餐饮类商户
	 * */
	public static boolean isFoodMchnt(String str){
		for(int i=0; i<FOOD_KEY.length; i++){
			if(str.contains(FOOD_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于第一类套码商户
	 * */
	public static boolean isFalse1Mchnt(String str){
		for(int i=0; i<FALSE_KEY1.length; i++){
			if(str.contains(FALSE_KEY1[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于第二类套码商户
	 * */
	public static boolean isFalse2Mchnt(String str){
		for(int i=0; i<FALSE_KEY2.length; i++){
			if(str.contains(FALSE_KEY2[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于健身馆类商户
	 * */
	public static boolean isFitMchnt(String str){
		for(int i=0; i<FIT_KEY.length; i++){
			if(str.contains(FIT_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于瑜珈类商户
	 * */
	public static boolean isYogaMchnt(String str){
		for(int i=0; i<YOGA_KEY.length; i++){
			if(str.contains(YOGA_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于高尔夫商户
	 * */
	public static boolean isGolfMchnt(String str){
		for(int i=0; i<GOLF_KEY.length; i++){
			if(str.contains(GOLF_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于网球场类商户
	 * */
	public static boolean isTennisMchnt(String str){
		for(int i=0; i<TENNIS_KEY.length; i++){
			if(str.contains(TENNIS_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于羽毛球类商户
	 * */
	public static boolean isBadmintonMchnt(String str){
		for(int i=0; i<BADMINTON_KEY.length; i++){
			if(str.contains(BADMINTON_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于跆拳道类商户
	 * */
	public static boolean isTaekwondoMchnt(String str){
		for(int i=0; i<TAEKWONDO_KEY.length; i++){
			if(str.contains(TAEKWONDO_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于保龄球类商户
	 * */
	public static boolean isBowlingMchnt(String str){
		for(int i=0; i<BOWLING_KEY.length; i++){
			if(str.contains(BOWLING_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于马术商户
	 * */
	public static boolean isHorsemanshipMchnt(String str){
		for(int i=0; i<HORSEMANSHIP_KEY.length; i++){
			if(str.contains(HORSEMANSHIP_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于击剑类商户
	 * */
	public static boolean isSwordplayMchnt(String str){
		for(int i=0; i<SWORDPLAY_KEY.length; i++){
			if(str.contains(SWORDPLAY_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于台球类商户
	 * */
	public static boolean isSnokeMchnt(String str){
		for(int i=0; i<SNOKE_KEY.length; i++){
			if(str.contains(SNOKE_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 根据关键字判断是否属于游泳馆商户
	 * */
	public static boolean isSwimMchnt(String str){
		for(int i=0; i<SWIM_KEY.length; i++){
			if(str.contains(SWIM_KEY[i]))
				return true;
		}
		return false;
	}
	
	/**
	 * 获取加油站品牌名
	 * 
	 * */
	public static String getRefuelBrand(String str){
		for(int i=0; i<REFUEL_BRAND_KEY.length; i++){
			if(str.contains("中国石油天然气"))
				return "中石油";
			else if(str.contains("中国石油化工"))
				return "中石化";
			else if(str.contains(REFUEL_BRAND_KEY[i]))
				return REAUEL_BRAND_MAP.get(REFUEL_BRAND_KEY[i]);
		}
		return "民营";
	}
	
	/**
	 * 计算该商户在娱乐夜生活标签里的所属类别
	 * 
	 * */
	public static String getEnttmTypeOfMchnt(String mchntName, String mcc){
		String type = TagUtility.FALSE_TYPE;
		
		if(TagUtility.isFalse1Mchnt(mchntName))
			return type;
		else if(TagUtility.isCafeMchnt(mchntName))						//咖啡馆
			return TagUtility.CAFE_TYPE;
		else if(TagUtility.isSnokeMchnt(mchntName))						//台球
			return TagUtility.SNOKE_TYPE;
		else if(TagUtility.isTeaMchnt(mchntName))						//茶馆
			return TagUtility.TEA_TYPE;
		else if(TagUtility.isKTVMchnt(mchntName))						//ktv
			return TagUtility.KTV_TYPE;	
		else if(TagUtility.isBarMchnt(mchntName))						//酒吧
			return TagUtility.BAR_TYPE;
			
		else if(TagUtility.isFitMchnt(mchntName))						//健身馆
			return TagUtility.FIT_TYPE;	
		else if(TagUtility.isYogaMchnt(mchntName))						//瑜珈馆
			return TagUtility.YOGA_TYPE;
		else if(TagUtility.isGolfMchnt(mchntName))						//高尔夫
			return TagUtility.GOLF_TYPE;
		else if(TagUtility.isTennisMchnt(mchntName))					//网球 
			return TagUtility.TENNIS_TYPE;
		else if(TagUtility.isBadmintonMchnt(mchntName))					//羽毛球、乒乓
			return TagUtility.BADMINTON_TYPE;
		else if(TagUtility.isTaekwondoMchnt(mchntName))					//跆拳道、武术
			return TagUtility.TAEKWONDO_TYPE;
		else if(TagUtility.isBowlingMchnt(mchntName))					//保龄球 
			return TagUtility.BOWLING_TYPE;
		else if(TagUtility.isHorsemanshipMchnt(mchntName))				//马术
			return TagUtility.HORSEMANSHIP_TYPE;
		else if(TagUtility.isSwordplayMchnt(mchntName))					//击剑
			return TagUtility.SWORDPLAY_TYPE;
		else if(TagUtility.isSwimMchnt(mchntName))						//游泳
			return TagUtility.SWIM_TYPE;
		else if(TagUtility.isMassageMchnt(mchntName))					//推拿洗浴
			return TagUtility.MASSAGE_TYPE;
		else if(TagUtility.isClubMchnt(mchntName))						//会所
			return TagUtility.CLUB_TYPE;
		else if(TagUtility.isFoodMchnt(mchntName))
			return type;
		else if(TagUtility.isFalse2Mchnt(mchntName))
			return type;
		else{
			if(mcc.equals("5813"))
				return TagUtility.BAR_TYPE;;
			if(mcc.equals("7911"))
				return TagUtility.CLUB_TYPE;
			if(mcc.equals("7297"))
				return TagUtility.MASSAGE_TYPE;
			if(mcc.equals("7932"))
				return TagUtility.SNOKE_TYPE;
			if(mcc.equals("7933"))
				return TagUtility.BOWLING_TYPE;
			if(mcc.equals("7992"))
				return TagUtility.GOLF_TYPE;
			if(mcc.equals("7997"))
				return TagUtility.SPORT_TYPE;
			if(mcc.equals("7903"))
				return TagUtility.FIT_TYPE;
			if(mcc.equals("7941"))
				return TagUtility.SPORT_TYPE;
		}
		return type;
	}
	
	
	/**
	 * 计算该商户在娱乐夜生活标签里的所属类别列表
	 * 
	 * */
//	public static HashSet<String> getEnttmTypeOfMchntList(String mchntName, String mcc){
//		HashSet<String> set = new HashSet<String>();
//		String type = TagUtility.FALSE_TYPE;
//		
//		if(TagUtility.isFalse1Mchnt(mchntName))
//			return type;
//		else if(TagUtility.isCafeMchnt(mchntName))						//咖啡馆
//			return TagUtility.CAFE_TYPE;
//		else if(TagUtility.isSnokeMchnt(mchntName))						//台球
//			return TagUtility.SNOKE_TYPE;
//		else if(TagUtility.isTeaMchnt(mchntName))						//茶馆
//			return TagUtility.TEA_TYPE;
//		else if(TagUtility.isKTVMchnt(mchntName))						//ktv
//			return TagUtility.KTV_TYPE;	
//		else if(TagUtility.isBarMchnt(mchntName))						//酒吧
//			return TagUtility.BAR_TYPE;
//		else if(TagUtility.isMassageMchnt(mchntName))					//推拿洗浴
//			return TagUtility.MASSAGE_TYPE;
//		else if(TagUtility.isClubMchnt(mchntName))						//会所
//			return TagUtility.CLUB_TYPE;
//		else if(TagUtility.isFitMchnt(mchntName))						//健身馆
//			return TagUtility.FIT_TYPE;	
//		else if(TagUtility.isYogaMchnt(mchntName))						//瑜珈馆
//			return TagUtility.YOGA_TYPE;
//		else if(TagUtility.isGolfMchnt(mchntName))						//高尔夫
//			return TagUtility.GOLF_TYPE;
//		else if(TagUtility.isTennisMchnt(mchntName))					//网球 
//			return TagUtility.TENNIS_TYPE;
//		else if(TagUtility.isBadmintonMchnt(mchntName))					//羽毛球、乒乓
//			return TagUtility.BADMINTON_TYPE;
//		else if(TagUtility.isTaekwondoMchnt(mchntName))					//跆拳道、武术
//			return TagUtility.TAEKWONDO_TYPE;
//		else if(TagUtility.isBowlingMchnt(mchntName))					//保龄球 
//			return TagUtility.BOWLING_TYPE;
//		else if(TagUtility.isHorsemanshipMchnt(mchntName))				//马术
//			return TagUtility.HORSEMANSHIP_TYPE;
//		else if(TagUtility.isSwordplayMchnt(mchntName))					//击剑
//			return TagUtility.SWORDPLAY_TYPE;
//		else if(TagUtility.isSwimMchnt(mchntName))						//游泳
//			return TagUtility.SWIM_TYPE;
//		else if(TagUtility.isFoodMchnt(mchntName))
//			return type;
//		else if(TagUtility.isFalse2Mchnt(mchntName))
//			return type;
//		else{
//			if(mcc.equals("5813"))
//				return TagUtility.BAR_TYPE;;
//			if(mcc.equals("7911"))
//				return TagUtility.CLUB_TYPE;
//			if(mcc.equals("7297"))
//				return TagUtility.MASSAGE_TYPE;
//			if(mcc.equals("7932"))
//				return TagUtility.SNOKE_TYPE;
//			if(mcc.equals("7933"))
//				return TagUtility.BOWLING_TYPE;
//			if(mcc.equals("7992"))
//				return TagUtility.GOLF_TYPE;
//			if(mcc.equals("7997"))
//				return TagUtility.SPORT_TYPE;
//			if(mcc.equals("7903"))
//				return TagUtility.FIT_TYPE;
//			if(mcc.equals("7941"))
//				return TagUtility.SPORT_TYPE;
//		}
//		return type;
//	}
	
	/**
	 * 计算该商户在体育运动标签里的所属类别
	 * 
	 * */
	public static String getSportTypeOfMchnt(String mchntName, String mcc){
		String type = TagUtility.FALSE_TYPE;
		
		return type;
	}
	
	public static void main(String[] args){
		String[] strs = {"tom","tom","john","jim","tom","john"};
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for(String str: strs){
			if(map.get(str)!=null)
			{
				int value = map.get(str);
				map.put(str, value+1);
			}
			else
				map.put(str, 1);
		}
		
		Iterator it = map.keySet().iterator();
		while(it.hasNext()){
			String key = (String)it.next();
			System.out.println(key+":"+map.get(key));
		}
		
		System.out.println(new TagUtility().getEnttmTypeOfMchnt("京胜兴怡和投资顾问有限公司", "7941"));
		
	}
	
}
//	