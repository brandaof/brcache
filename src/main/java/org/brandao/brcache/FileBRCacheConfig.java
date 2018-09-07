package org.brandao.brcache;

import org.brandao.brcache.memory.Memory;

/**
 * Define a configuração de um cache a partir de um arquivo.
 * <pre>
 * ex:
 *     Configuration configuration = ...;
 *     Cache cache = new Cache(new PropertiesBRCacheConfig(configuration));
 * </pre>
 * @author Brandao
 *
 */
public class FileBRCacheConfig 
	extends BRCacheConfig{

	private static final long serialVersionUID = 7110634582865791095L;

	private static final String MEMORY_PREFIX = "org.brandao.brcache.memory.";

	private static final String MEMORY_SUFFIX = "Memory";

	private Configuration configuration;
	
	public FileBRCacheConfig(Configuration value){
		this.apply(value);
	}
	
    private void apply(Configuration config){
    	
    	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    	
    	this.configuration   = config;
        this.nodesBufferSize = config.getLong(CacheConstants.NODES_BUFFER_SIZE,		"1m");
        this.nodesPageSize   = config.getLong(CacheConstants.NODES_PAGE_SIZE,		"1k");
        this.nodesSwapFactor = config.getDouble(CacheConstants.NODES_SWAP_FACTOR,	"0.3");
        this.indexBufferSize = config.getLong(CacheConstants.INDEX_BUFFER_SIZE,		"1m");
        this.indexPageSize   = config.getLong(CacheConstants.INDEX_PAGE_SIZE,		"1k");
        this.indexSwapFactor = config.getDouble(CacheConstants.INDEX_SWAP_FACTOR,	"0.3");
        this.dataBufferSize  = config.getLong(CacheConstants.DATA_BUFFER_SIZE,		"64m");
        this.dataBlockSize   = config.getLong(CacheConstants.DATA_BLOCK_SIZE,		"1k");
        this.dataPageSize    = config.getLong(CacheConstants.DATA_PAGE_SIZE,		"8k");
        this.dataSwapFactor  = config.getDouble(CacheConstants.DATA_SWAP_FACTOR,	"0.3");
        this.maxSizeEntry    = config.getLong(CacheConstants.MAX_SIZE_ENTRY,		"1m");
        this.maxSizeKey      = config.getInt(CacheConstants.MAX_SIZE_KEY,			"100");
        this.swapperThread   = config.getInt(CacheConstants.SWAPPER_THREAD,			"4");
        this.dataPath        = config.getString(CacheConstants.DATA_PATH,			"/mnt/brcache");
        this.memory          = this.createMemory(config.getString(CacheConstants.MEMORY_ACCESS_TYPE,"heap"), classLoader);
        
    }

    /**
     * Obtém os metadados de configuração.
     * @return metadados.
     */
	public Configuration getConfiguration() {
		return configuration;
	}
    
    @SuppressWarnings("unchecked")
    protected Memory createMemory(String name, ClassLoader classLoader){
    	try{
        	String className = 
        			MEMORY_PREFIX + 
        			Character.toUpperCase(name.charAt(0)) + name.substring(1, name.length()).toLowerCase() +
        			MEMORY_SUFFIX;
        	
        	Class<Memory> clazz = (Class<Memory>)Class.forName(className, true, classLoader);
    		return clazz.newInstance();
    	}
    	catch(Throwable e){
    		throw new IllegalStateException("invalid memory type: " + name, e);
    	}
    }
    
}
