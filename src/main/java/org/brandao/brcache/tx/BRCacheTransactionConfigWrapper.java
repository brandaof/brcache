package org.brandao.brcache.tx;

import org.brandao.brcache.Configuration;
import org.brandao.brcache.collections.Swapper;
import org.brandao.brcache.memory.Memory;

class BRCacheTransactionConfigWrapper extends BRCacheTransactionConfig{

	private static final long serialVersionUID = -5763521134169834371L;

	private BRCacheTransactionConfig wrapper;
	
	private String dataPath;
	
	public BRCacheTransactionConfigWrapper(BRCacheTransactionConfig wrapper, String dataPath){
		this.wrapper = wrapper;
		this.dataPath = dataPath;
	}
	public Configuration getConfiguration() {
		return wrapper.getConfiguration();
	}

	public String getDataPath() {
		return this.dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public long getNodesBufferSize() {
		return wrapper.getNodesBufferSize();
	}

	public void setNodesBufferSize(long nodesBufferSize) {
		wrapper.setNodesBufferSize(nodesBufferSize);
	}

	public long getNodesPageSize() {
		return wrapper.getNodesPageSize();
	}

	public void setNodesPageSize(long nodesPageSize) {
		wrapper.setNodesPageSize(nodesPageSize);
	}

	public double getNodesSwapFactor() {
		return wrapper.getNodesSwapFactor();
	}

	public void setNodesSwapFactor(double nodesSwapFactor) {
		wrapper.setNodesSwapFactor(nodesSwapFactor);
	}

	public long getIndexBufferSize() {
		return wrapper.getIndexBufferSize();
	}

	public void setIndexBufferSize(long indexBufferSize) {
		wrapper.setIndexBufferSize(indexBufferSize);
	}

	public long getIndexPageSize() {
		return wrapper.getIndexPageSize();
	}

	public void setIndexPageSize(long indexPageSize) {
		wrapper.setIndexPageSize(indexPageSize);
	}

	public double getIndexSwapFactor() {
		return wrapper.getIndexSwapFactor();
	}

	public void setIndexSwapFactor(double indexSwapFactor) {
		wrapper.setIndexSwapFactor(indexSwapFactor);
	}

	public long getDataBufferSize() {
		return wrapper.getDataBufferSize();
	}

	public void setDataBufferSize(long dataBufferSize) {
		this.wrapper.setDataBufferSize(dataBufferSize);
	}

	public long getDataBlockSize() {
		return wrapper.getDataBlockSize();
	}

	public void setDataBlockSize(long dataBlockSize) {
		wrapper.setDataBlockSize(dataBlockSize);
	}

	public long getDataPageSize() {
		return wrapper.getDataPageSize();
	}

	public void setDataPageSize(long dataPageSize) {
		wrapper.setDataPageSize(dataPageSize);
	}

	public double getDataSwapFactor() {
		return wrapper.getDataSwapFactor();
	}

	public void setDataSwapFactor(double dataSwapFactor) {
		wrapper.setDataSwapFactor(dataSwapFactor);
	}

	public long getMaxSizeEntry() {
		return wrapper.getMaxSizeEntry();
	}

	public void setMaxSizeEntry(long maxSizeEntry) {
		wrapper.setMaxSizeEntry(maxSizeEntry);
	}

	public int getMaxSizeKey() {
		return wrapper.getMaxSizeKey();
	}

	public void setMaxSizeKey(int maxSizeKey) {
		wrapper.setMaxSizeKey(maxSizeKey);
	}

	public int getSwapperThread() {
		return wrapper.getSwapperThread();
	}

	public void setSwapperThread(int swapperThread) {
		wrapper.setSwapperThread(swapperThread);
	}

	public Swapper getSwapper() {
		return wrapper.getSwapper();
	}

	public void setSwapper(Swapper swapper) {
		wrapper.setSwapper(swapper);
	}

	public Memory getMemory() {
		return wrapper.getMemory();
	}

	public void setMemory(Memory memory) {
		this.wrapper.setMemory(memory);
	}
	
}
