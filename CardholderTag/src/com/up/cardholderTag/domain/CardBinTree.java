package com.up.cardholderTag.domain;



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
	 * 添加卡BIN到当前的树节点中，可以用来添加和更新
	 * 
	 * @param cardBin
	 */
	public void addNode(CardBinInfo cardBin) {
		rootNode.addSubNode(cardBin.getCardBin(), cardBin);
	}

	/**
	 * 删除卡BIN只是删除对应的节点中的CardBin为null
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