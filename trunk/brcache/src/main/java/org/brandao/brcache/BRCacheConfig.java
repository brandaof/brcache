package org.brandao.brcache;

import java.io.Serializable;

import org.brandao.brcache.SwaperStrategy;

public class BRCacheConfig implements Serializable{

	private static final long serialVersionUID = 9065603898804344980L;

	protected long nodesBufferSize;
    
	protected long nodesPageSize;
    
	protected double nodesSwapFactor;
    
	protected long indexBufferSize;
    
	protected long indexPageSize;
    
	protected double indexSwapFactor;
    
	protected long dataBufferSize;
    
	protected long dataBlockSize;
    
	protected long dataPageSize;
    
	protected double dataSwapFactor;
	
	protected long maxSizeEntry;
    
    protected int maxSizeKey;
    
    protected int swapperThread;
    
    protected SwaperStrategy swapper;
    
    protected String dataPath;
    
    protected Configuration configuration;
    
    public BRCacheConfig(){
    }

    public void setConfiguration(Configuration config){
    	this.configuration       = config;
        this.nodesBufferSize     = config.getLong("nodes_buffer_size",		"3k");
        this.nodesPageSize       = config.getLong("nodes_page_size",		"1k");
        this.nodesSwapFactor     = config.getDouble("nodes_swap_factor",	"0.1");
        this.indexBufferSize     = config.getLong("index_buffer_size",		"1k");
        this.indexPageSize       = config.getLong("index_page_size",		"512b");
        this.indexSwapFactor     = config.getDouble("index_swap_factor",	"0.1");
        this.dataBufferSize      = config.getLong("data_buffer_size",		"6k");
        this.dataBlockSize       = config.getLong("data_block_size",		"512b");
        this.dataPageSize        = config.getLong("data_page_size",			"64b");
        this.dataSwapFactor      = config.getDouble("data_swap_factor",		"0.1");
        this.maxSizeEntry        = config.getLong("max_size_entry",			"1m");
        this.maxSizeKey          = config.getInt("max_size_key",			"48");
        this.swapperThread       = config.getInt("swapper_thread",			"1");
        this.dataPath            = config.getString("data_path",			"/var/brcache");
        this.swapper             = SwaperStrategy.valueOf(config.getString("swapper_type","file"));
    }
    
	public Configuration getConfiguration() {
		return configuration;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
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
