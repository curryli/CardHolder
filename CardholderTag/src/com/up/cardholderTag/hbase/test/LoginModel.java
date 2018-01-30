package com.up.cardholderTag.hbase.test;

public class LoginModel {
	/**
	 * 用户名
	 */
	private String userNm;
	/**
	 * 密码
	 */
	private String userPwd;
	/**
	 * 机构号
	 */
	private String insCd;
	
	public String getUserNm() {
		return userNm;
	}
	public void setUserNm(String userNm) {
		this.userNm = userNm;
	}
	public String getUserPwd() {
		return userPwd;
	}
	public void setUserPwd(String userPwd) {
		this.userPwd = userPwd;
	}
	public String getInsCd() {
		return insCd;
	}
	public void setInsCd(String insCd) {
		this.insCd = insCd;
	}
		
}
