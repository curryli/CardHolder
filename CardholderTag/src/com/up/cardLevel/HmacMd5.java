package com.up.cardLevel;



import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.io.Text;

/**
 * MD5算法 作用：将一字符串加密成32位16进制字符串；
 * 
 */
public class HmacMd5{
	  public static final String key="a13049c9f1874327b8a1257ee553f0a9";
	@SuppressWarnings("finally")
	public Text evaluate(Object args) throws NoSuchAlgorithmException {
		if (args == null)
			args = "";
		Text result=null;
		try {
			result= getHmacMd5Bytes(args.toString().getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			return result;
		}
	}

	  public static Text getHmacMd5Bytes(byte[] data)
			    throws Exception
			  {
			    String result = null;
			    SecretKey sk = new SecretKeySpec(key.getBytes("UTF-8"), "HmacMD5");
			    Mac mac = Mac.getInstance("HmacMD5");
			    mac.init(sk);
			    byte[] temp = mac.doFinal(data);
			    StringBuffer sb = new StringBuffer();
			    for (byte b : temp)
			    {
			      if (Integer.toHexString(0xFF & b).length() == 1) {
			        sb = sb.append("0" + Integer.toHexString(0xFF & b));
			      } else {
			        sb = sb.append(Integer.toHexString(0xFF & b));
			      }
			      result = sb.toString();
			    }
			    return new Text(result);
			  }

	public static void main(String args[]) throws Exception {
		  HmacMd5 rand=new HmacMd5();
		  System.out.println(rand.evaluate(null));
		  System.out.println(rand.evaluate("ABCD"));
		  System.out.println(rand.evaluate("BCDA"));
		  System.out.println(rand.evaluate("1"));
	}
}
