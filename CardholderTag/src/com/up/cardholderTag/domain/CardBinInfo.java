package com.up.cardholderTag.domain;



public class CardBinInfo {

	private String cardBin;

	private String cardATTR;

	private String cardMedia;

	private String cardCnName = "";

	private String cardEnName = "";

	private String issuerInstCode;

	private String cardNature;

	private String cardClass;

	private String cardBrand;

	private String cardProduct;

	private String cardLevel;

	public String getCardNature() {
		return cardNature;
	}

	public void setCardNature(String cardNature) {
		this.cardNature = cardNature;
	}

	public String getCardATTR() {
		return cardATTR == null ? "00" : cardATTR;
	}

	public void setCardATTR(String cardATTR) {
		this.cardATTR = cardATTR;
	}

	public String getCardMedia() {
		return cardMedia == null ? "0" : cardMedia;
	}

	public void setCardMedia(String cardMedia) {
		this.cardMedia = cardMedia;
	}

	public String getCardProduct() {
		return cardProduct == null ? "00" : cardProduct;
	}

	public void setCardProduct(String cardProduct) {
		this.cardProduct = cardProduct;
	}

	public String getCardLevel() {
		return cardLevel == null ? "0" : cardLevel;
	}

	public void setCardLevel(String cardLevel) {
		this.cardLevel = cardLevel;
	}

	public String getCardBin() {
		return cardBin.trim();
	}

	public void setCardBin(String cardBin) {
		this.cardBin = cardBin;
	}

	public String getCardCnName() {
		return cardCnName.trim();
	}

	public void setCardCnName(String cardCnName) {
		this.cardCnName = cardCnName;
	}

	public String getCardEnName() {
		return cardEnName.trim();
	}

	public void setCardEnName(String cardEnName) {
		this.cardEnName = cardEnName;
	}

	public String getIssuerInstCode() {
		return issuerInstCode;
	}

	public void setIssuerInstCode(String issuerInstCode) {
		this.issuerInstCode = issuerInstCode;
	}

	public String getCardClass() {
		return cardClass;
	}

	public void setCardClass(String cardClass) {
		this.cardClass = cardClass;
	}

	public String getCardBrand() {
		return cardBrand == null ? "00" : cardBrand;
	}

	public void setCardBrand(String cardBrand) {
		this.cardBrand = cardBrand;
	}


}
