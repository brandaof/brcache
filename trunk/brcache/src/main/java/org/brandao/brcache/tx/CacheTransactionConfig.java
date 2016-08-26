package org.brandao.brcache.tx;

import org.brandao.brcache.Configuration;
import org.brandao.brcache.SwaperStrategy;

public class CacheTransactionConfig {

    private long nodesBufferSize;
    
    private long nodesPageSize;
    
    private double nodesSwapFactor;
    
    private long indexBufferSize;
    
    private long indexPageSize;
    
    private double indexSwapFactor;
    
    private long dataBufferSize;
    
    private long dataBlockSize;
    
    private long dataPageSize;
    
    private double dataSwapFactor;
	
    private long maxSizeEntry;
    
    private int maxSizeKey;
    
    private int swapperThread;
    
    private SwaperStrategy swapper;
    
    public CacheTransactionConfig(Configuration config){
        this.nodesBufferSize     = config.getLong("transaction_nodes_buffer_size","3k");
        this.nodesPageSize       = config.getLong("transaction_nodes_page_size","1k");
        this.nodesSwapFactor     = config.getDouble("transaction_nodes_swap_factor","0.1");
        this.indexBufferSize     = config.getLong("transaction_index_buffer_size","1k");
        this.indexPageSize       = config.getLong("transaction_index_page_size","512b");
        this.indexSwapFactor     = config.getDouble("transaction_index_swap_factor","0.1");
        this.dataBufferSize      = config.getLong("transaction_data_buffer_size","6k");
        this.dataBlockSize       = config.getLong("transaction_data_block_size","512b");
        this.dataPageSize        = config.getLong("transaction_data_page_size","64b");
        this.dataSwapFactor      = config.getDouble("transaction_data_swap_factor","0.1");
        this.maxSizeEntry        = config.getLong("max_size_entry","1m");
        this.maxSizeKey          = config.getInt("max_size_key","48");
        this.swapperThread       = 1;
        this.swapper             = SwaperStrategy.valueOf(config.getString("swapper_type","file"));
    }

	public long getNodesBufferSize() {
		return nodesBufferSize;
	}

	public void setNodesBufferSize(long nodesBufferSize) {
		this.nodesBufferSize = nodesBufferSize;
	}

	public long getNodesPageSize() {
		return nodesPageSize;
	}

	public void setNodesPageSize(long nodesPageSize) {
		this.nodesPageSize = nodesPageSize;
	}

	public double getNodesSwapFactor() {
		return nodesSwapFactor;
	}

	public void setNodesSwapFactor(double nodesSwapFactor) {
		this.nodesSwapFactor = nodesSwapFactor;
	}

	public long getIndexBufferSize() {
		return indexBufferSize;
	}

	public void setIndexBufferSize(long indexBufferSize) {
		this.indexBufferSize = indexBufferSize;
	}

	public long getIndexPageSize() {
		return indexPageSize;
	}

	public void setIndexPageSize(long indexPageSize) {
		this.indexPageSize = indexPageSize;
	}

	public double getIndexSwapFactor() {
		return indexSwapFactor;
	}

	public void setIndexSwapFactor(double indexSwapFactor) {
		this.indexSwapFactor = indexSwapFactor;
	}

	public long getDataBufferSize() {
		return dataBufferSize;
	}

	public void setDataBufferSize(long dataBufferSize) {
		this.dataBufferSize = dataBufferSize;
	}

	public long getDataBlockSize() {
		return dataBlockSize;
	}

	public void setDataBlockSize(long dataBlockSize) {
		this.dataBlockSize = dataBlockSize;
	}

	public long getDataPageSize() {
		return dataPageSize;
	}

	public void setDataPageSize(long dataPageSize) {
		this.dataPageSize = dataPageSize;
	}

	public double getDataSwapFactor() {
		return dataSwapFactor;
	}

	public void setDataSwapFactor(double dataSwapFactor) {
		this.dataSwapFactor = dataSwapFactor;
	}

	public long getMaxSizeEntry() {
		return maxSizeEntry;
	}

	public void setMaxSizeEntry(long maxSizeEntry) {
		this.maxSizeEntry = maxSizeEntry;
	}

	public int getMaxSizeKey() {
		return maxSizeKey;
	}

	public void setMaxSizeKey(int maxSizeKey) {
		this.maxSizeKey = maxSizeKey;
	}

	public int getSwapperThread() {
		return swapperThread;
	}

	public void setSwapperThread(int swapperThread) {
		this.swapperThread = swapperThread;
	}

	public SwaperStrategy getSwapper() {
		return swapper;
	}

	public void setSwapper(SwaperStrategy swapper) {
		this.swapper = swapper;
	}
    
}
