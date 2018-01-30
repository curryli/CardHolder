package com.up.cardholderTag.hbase.test;

import org.apache.hadoop.hbase.client.Result;

public class LoginTest {

	public static void main(String[] args) {
		String usrNm = "yjy-01";
		String pwd = "123456";
		String servletFlag="";
		String loginFlag="";
		
		HBaseService hbs = new HBaseService();
		LoginModel model = new LoginModel();
		Result res = hbs.get("tbl_cardholder_sys_user", usrNm, "d");
		model = hbs.getLoginModel(res);
		
		if (model == null) {
			return;
		}
		
		if(pwd!=null && pwd.equals(model.getUserPwd()) /*&& ins.equals(model.getInsCd())*/) {
			servletFlag = "1";
			loginFlag = "1";
		} else {
			servletFlag = "1";
			loginFlag = "0";
		}
		
		System.out.println("LoginServlet: servletFlag: " + servletFlag);
		System.out.println("LoginServlet: loginFlag: " + loginFlag);

	}

}
