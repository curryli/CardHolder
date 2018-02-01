package cardbin;

public class CardBinInfo {

	private String cardBinTp;

	private String cardBin;

	private int cardLenStart;

	private int cardLen;

	private long cardBegin;

	private long cardEnd;

	private String cardLvl;

	public String getCardBinTp() {
		return cardBinTp.trim();
	}

	public void setCardBinTp(String cardBinTp) {
		this.cardBinTp = cardBinTp;
	}

	public String getCardBin() {
		return cardBin.trim();
	}

	public void setCardBin(String cardBin) {
		this.cardBin = cardBin;
	}

	public int getCardLenStart() {
		return cardLenStart;
	}

	public void setCardLenStart(int cardLenStart) {
		this.cardLenStart = cardLenStart;
	}

	public int getCardLen() {
		return cardLen;
	}

	public void setCardLen(int cardLen) {
		this.cardLen = cardLen;
	}

	public long getCardBegin() {
		return cardBegin;
	}

	public void setCardBegin(long cardBegin) {
		this.cardBegin = cardBegin;
	}

	public long getCardEnd() {
		return cardEnd;
	}

	public void setCardEnd(long cardEnd) {
		this.cardEnd = cardEnd;
	}

	public String getCardLvl() {
		return cardLvl.trim();
	}

	public void setCardLvl(String cardLvl) {
		this.cardLvl = cardLvl;
	}

}
