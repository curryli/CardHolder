package com.up.cardLevel;

import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * HMACMD5工具类
 * */
public class HmacMd5Util 
{
	public static final int INDEX = 36;
	public static final int LENGTH = 196;
	public static final int BF_SIZE = 2048;
	public static final int BS_SIZE = 2048;
	public static final int ZERO = 0;
	public static final String endLine = "\n";
	public static final String separator = "/";
	public static final String maohao_douhao_maohao = "\",\"";
	public static final String md5_key = "a13049c9f1874327b8a1257ee553f0a9";
	
	public static final int OS_BUF = 65536;//1048576//524288//262144//131072//65536
	public static final int number = 500;

	public static final String HTTPFS_HOST = "172.18.160.12";
	
	public static final String UTF8 = "UTF-8";
	
	public static String getHmacMd5Bytes(byte[] key,byte[] data)throws Exception
	{
		String result = null;
		SecretKey sk = new SecretKeySpec(key,"HmacMD5");
		Mac mac = Mac.getInstance("HmacMD5");
		mac.init(sk);
		byte[] temp = mac.doFinal(data);
		StringBuffer sb = new StringBuffer();
		for(byte b : temp)
		{
			if(Integer.toHexString(0xFF & b).length() == 1)
			{
				sb = sb.append("0"+Integer.toHexString(0xFF & b));
			}
			else
			{
				sb = sb.append(Integer.toHexString(0xFF & b));
			}
			result = sb.toString();
		}
		return result;
	}
	
	public static String getHmacMd5Bytes2(byte[] key, byte[] data) 
			throws Exception 
		{ 
			String result = null; 
			SecretKey sk = new SecretKeySpec(key, "HmacMD5"); 
			Mac mac = Mac.getInstance("HmacMD5"); 
			mac.init(sk); 
			byte[] temp = mac.doFinal(data); 
			StringBuffer sb = new StringBuffer(); 
			for (byte b : temp) 
			{ 
			if (Integer.toHexString(0xFF & b).length() == 1) 
			{ 
			sb = sb.append("0" + Integer.toHexString(0xFF & b)); 
			} 
			else 
			{ 
			sb = sb.append(Integer.toHexString(0xFF & b)); 
			} 
			result = sb.toString(); 
			} 
			return result; 
		}

	public static String reverse(String cardId){
		StringBuilder sb = new StringBuilder();
		sb.append(cardId);
		sb.reverse();
		return sb.toString().trim();
	}
	
	/*public static void main(String[] args) throws Exception 
	{
		System.out.println(UUID.randomUUID().toString().replace("-", ""));
		
		String dd = "\"00013930   6501761216133943000163930\",\"01\",\"0\",\"0\",\"54\",\"00013930                   000163930\",\"00013930   6501761216133943000163930\",\"0\",\"0\",\"1\",\"0\",\"0\",\"\",\"0\",\"1\",\"3930\",\"3930\",\"1\",\"1\",\"01\",\"01\",\"0\",\"5\",\"00273930\",\"01\",\"00163930\",\"01\",\"61023930\",\"01\",\"01020000\",\"01\",\"\",\"\",\"00013930\",\"01\",\"******\",\"61994d9275323810dd1b7c28498cba06\",\"650176\",\"\",\"\",\"\",\"N\",\"51\",\"51\",\"51\",\"51\",\"10000\",\"0\",\"P02\",\"P020000000\",\"14\",\"4\",\"\",\"\",\"\",\"P02\",\"20131216\",\"12\",\"16\",\"\",\"00273930\",\"61023930\",\"11793.0\",\"0\",\"0\",\"156\",\"0\",\"0\",\"\",\"\",\"\",\"\",\"0\",\"1\",\"0200\",\"0200\",\"199558804100\",\"\",\"190000\",\"190000\",\"1216133943\",\"133943\",\"1216\",\"\",\"4900\",\"012\",\"\",\"81\",\"81\",\"133943650176\",\"70000462\",\"14\",\"805920004294006\",\"厦门水务集团有限公司\",\"\",\"0000\",\"0000140000\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"00\",\"pa2\",\"0\",\"0\",\"1\",\"00000000000000000000\",\"\",\"\",\"2013-12-16 13:40:36.268605\",\"0\",\"74\",\"01\",\"\",\"\",\"\",\"1301:000000302817\",\"\",\"\",\"\",\"10120\",\"10120\",\"61000000\",\"61000000\",\"156\",\"156\",\"\",\"\",\"0\",\"0\",\"0\",\"0\",\"\",\"01053930\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"\",\"\",\"\",\"\",\"\",\"2013-12-16 23:36:05.386097\",\"2013-12-16 23:36:05.386097\",\"\"";
		System.out.println(dd.getBytes("UTF-8").length);
		System.out.println(dd.length());
		
//		String a = "\"00013930   6501761216133943000163930      \",\"01\",\"0 \",\"0\",\"54\",\"00013930                   000163930      \",\"00013930   6501761216133943000163930      \",\"0\",\"0\",\"1\",\"0\",\"0\",\" \",\"0\",\"1\",\"3930\",\"3930\",\"1\",\"1\",\"01\",\"01\",\"0\",\"5\",\"00273930   \",\"01\",\"00163930   \",\"01\",\"61023930   \",\"01\",\"01020000   \",\"01\",\"           \",\"  \",\"00013930   \",\"01\",\"******\",\"199558804100187190597\",\"650176\",\"      \",\"         \",\"    \",\"N      \",\"51\",\"51\",\"51\",\"51\",\"10000\",\"0\",\"P02\",\"P020000000\",\"14\",\"4\",\" \",\"    \",\"   \",\"P02\",\"20131216\",\"12\",\"16\",\"        \",\"00273930   \",\"61023930   \",\"11793.0\",\"0\",\"0\",\"156\",\"0\",\"0\",\"   \",\"                                          \",\" \",\" \",\"                     0                                      \",\"1\",\"0200\",\"0200\",\"199558804100  \",\"              \",\"190000\",\"190000\",\"1216133943\",\"133943\",\"1216\",\"    \",\"4900\",\"012\",\"   \",\"81\",\"81\",\"133943650176\",\"70000462\",\"14\",\"805920004294006\",\"厦门水务集团有限公司                    \",\"                                                                                                       \",\"0000\",\"0000140000 \",\"    \",\"    \",\"      \",\"      \",\"          \",\"   \",\"  \",\"   \",\"   \",\"  \",\" \",\" \",\"\",\"\",\"\",\"\",\"\",\"00\",\"pa2\",\"0   \",\"0\",\"1\",\"00000000000000000000\",\" \",\"    \",\"2013-12-16 13:40:36.268605\",\"0\",\"74\",\"01\",\"  \",\"  \",\" \",\"1301:000000302817   \",\"                                                                                                    \",\" \",\"   \",\"10120\",\"10120\",\"61000000\",\"61000000\",\"156\",\"156\",\"     \",\"     \",\"0\",\"0\",\"0\",\"0\",\"               \",\"01053930   \",\"           \",\"           \",\"           \",\"           \",\"           \",\"           \",\"           \",\"           \",\"           \",\"           \",\"           \",\"           \",\"           \",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\" \",\"               \",\" \",\"  \",\"  \",\"2013-12-16 23:36:05.386097\",\"2013-12-16 23:36:05.386097\",\" \"";
//		System.out.println(a.length());
//		
//		System.out.println(Arrays.toString(DirectSplit(a)).length());
//		String[] as = DirectSplit(a);
//		System.out.println(as.length);
//		StringBuffer sb = new StringBuffer(2048);
//		for(String ss : as)
//		{
//			sb = sb.append(ss);
//		}
//		String temp = sb.toString();
//		System.out.println("temp.length()A:"+temp.length());
//		System.out.println(temp);
//		System.out.println(temp.replaceAll(",", ""));
//		System.out.println("temp.length()B:"+temp.length());
		//temp = temp.replaceAll("", "");
//		System.out.println(as[1].length());
		
		String path = "/user/test/";
		//去除末尾"/"
		if(path.endsWith(separator))
		{
			path = path.substring(0,path.length()-1);
		}
		
		// test123
		String directoryName = path.substring(path.lastIndexOf(separator) + 1);
		System.out.println("directoryName:"+directoryName);
		
		// /user/test
		String directory = path.substring(0, path.lastIndexOf(separator) + 1);
		System.out.println("directory:"+directory);
		
		//查询文件夹内容
		//String getRequest = PREFIX + directory + "?op=liststatus&user.name=hdfs";
	}*/
	
	public static void main(String[] args) throws Exception{
//		String cardNum="1762312301000074596";
//		String cardNum="6954700001032132671";
//		String cardNum="196222620140011614755";
//		String cardNum="557416110041026222619";
		String key = "a13049c9f1874327b8a1257ee553f0a9";
		String cardNum="196228480838067979370";
//		String cardNum="18622960866007613717";
		System.out.println(cardNum);
		cardNum = HmacMd5Util.reverse(cardNum);
		System.out.println(cardNum);
		
		System.out.println(HmacMd5Util.getHmacMd5Bytes(key.getBytes(),cardNum.trim().getBytes(HmacMd5Util.UTF8)));
		System.out.println(HmacMd5Util.getHmacMd5Bytes2(key.getBytes(),cardNum.trim().getBytes(HmacMd5Util.UTF8)));
	}
}
