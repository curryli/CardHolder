package cardbin;

import java.util.List;


public class CardBinTree {

	CardBinTreeNode rootNode = new CardBinTreeNode();

	public CardBinTree(List<CardBinInfo> cardBins) {

		for (CardBinInfo cardBin : cardBins) {
			rootNode.addSubNode(cardBin.getCardBin(), cardBin);
		}
	}

	public CardBinTree() {
		rootNode = new CardBinTreeNode();
	}

	public CardBinInfo traverseTree(String pan) {

		return rootNode.returnCardBin(pan);
	}

	public CardBinTreeNode getRootNode() {
		return rootNode;
	}

	public void setRootNode(CardBinTreeNode rootNode) {
		this.rootNode = rootNode;
	}

	/**
	 * ��ӿ�BIN����ǰ�����ڵ��У�����������Ӻ͸���
	 * 
	 * @param cardBin
	 */
	public void addNode(CardBinInfo cardBin) {
		rootNode.addSubNode(cardBin.getCardBin(), cardBin);
	}

	/**
	 * ɾ����BINֻ��ɾ����Ӧ�Ľڵ��е�CardBinΪnull
	 * 
	 * @param binCode
	 */
	public void removeNode(String binCode) {
		CardBinTreeNode node = rootNode.findNode(binCode);
		if (node != null && node.getCardBin() != null) {
			node.setCardBin(null);
		}
	}
}