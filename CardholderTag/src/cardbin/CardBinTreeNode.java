package cardbin;



public class CardBinTreeNode {

	private CardBinInfo cardBin;

	private CardBinTreeNode[] child = new CardBinTreeNode[10];

	public CardBinInfo getCardBin() {
		return cardBin;
	}

	public void setCardBin(CardBinInfo cardBin) {
		this.cardBin = cardBin;
	}

	public CardBinTreeNode[] getChild() {
		return child;
	}

	public void setChild(CardBinTreeNode[] child) {
		this.child = child;
	}

	public void addSubNode(String subCardBin, CardBinInfo cardBin) {
		if (subCardBin.length() == 0) {
			this.cardBin = cardBin;
		} else {
			char key = subCardBin.charAt(0);
			int num = key - '0';
			CardBinTreeNode subTreeNode;
			if (child[num] != null) {
				subTreeNode = child[num];
			} else {
				subTreeNode = new CardBinTreeNode();
				child[num] = subTreeNode;
			}
			subTreeNode.addSubNode(subCardBin.substring(1), cardBin);
		}
	}

	public CardBinInfo returnCardBin(String subPan) {

		// System.out.println("PAN===>" + subPan);
		if (subPan.length() == 0)
			return cardBin;

		char key = subPan.charAt(0);

		CardBinTreeNode subTreeNode;
		int num = key - '0';
		if (child[num] != null) {
			subTreeNode = child[num];
			CardBinInfo bin = subTreeNode.returnCardBin(subPan.substring(1));
			if (bin != null)
				return bin;
		}
		return cardBin;
	}

	public CardBinTreeNode findNode(String subPan) {
		if (subPan.length() == 0)
			return this;

		char key = subPan.charAt(0);

		CardBinTreeNode subTreeNode;
		int num = key - '0';
		if (child[num] != null) {
			subTreeNode = child[num];
			CardBinTreeNode node = subTreeNode.findNode(subPan.substring(1));
			if (node.getCardBin() != null)
				return node;
		}
		return this;
	}

}