package org.brandao.brcache;

import java.io.Serializable;

import org.brandao.brcache.SwaperStrategy;

/**
 * Configuração de um cache.
 * <pre>
 * ex:
 *     BRCacheConfig config = new BRCacheConfig();
 *     ...
 *     Cache cache = new Cache(config);
 * </pre>
 * @author Brandao
 *
 */
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

    public BRCacheConfig(Configuration config){
    	this.setConfiguration(config);
    }
    
    /**
     * Define os metadados de configuração.
     * @param config metadados.
     */
    public void setConfiguration(Configuration config){

    	this.configuration       = config;
        this.nodesBufferSize     = config.getLong("nodes_buffer_size",		"1m");
        this.nodesPageSize       = config.getLong("nodes_page_size",		"1k");
        this.nodesSwapFactor     = config.getDouble("nodes_swap_factor",	"0.5");
        this.indexBufferSize     = config.getLong("index_buffer_size",		"1m");
        this.indexPageSize       = config.getLong("index_page_size",		"1k");
        this.indexSwapFactor     = config.getDouble("index_swap_factor",	"0.5");
        this.dataBufferSize      = config.getLong("data_buffer_size",		"64m");
        this.dataBlockSize       = config.getLong("data_block_size",		"1k");
        this.dataPageSize        = config.getLong("data_page_size",			"8k");
        this.dataSwapFactor      = config.getDouble("data_swap_factor",		"0.5");
        this.maxSizeEntry        = config.getLong("max_size_entry",			"1m");
        this.maxSizeKey          = config.getInt("max_size_key",			"100");
        this.swapperThread       = config.getInt("swapper_thread",			"4");
        this.dataPath            = config.getString("data_path",			"/mnt/brcache");
        this.swapper             = SwaperStrategy.valueOf(config.getString("swapper_type","FILE").toUpperCase());
    }
    
    /**
     * Obtém os metadados de configuração.
     * @return metadados.
     */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Obtém a pasta onde o servidor irá fazer o swap dos dados quando 
	 * o limite da memória for atingido.
	 * @return pasta.
	 */
	public String getDataPath() {
		return dataPath;
	}

	/**
	 * Define a pasta onde o servidor irá fazer o swap dos dados quando 
	 * o limite da memória for atingido.
	 * @param dataPath pasta.
	 */
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	/**
	 * Obtém o tamanho do buffer usado para armazenar os nós na memória.
	 * @return tamanho em bytes
	 */
	public long getNodesBufferSize() {
		return nodesBufferSize;
	}

	/**
	 * Define otamanho do buffer usado para armazenar os nós na memória.
	 * @param nodesBufferSize tamanho em bytes.
	 */
	public void setNodesBufferSize(long nodesBufferSize) {
		this.nodesBufferSize = nodesBufferSize;
	}

	/**
	 * Obtém o tamanho da página do buffer dos nós.
	 * @return tamanho em bytes
	 */
	public long getNodesPageSize() {
		return nodesPageSize;
	}

	/**
	 * Define o tamanho da página do buffer dos nós.
	 * @param nodesPageSize
	 */
	public void setNodesPageSize(long nodesPageSize) {
		this.nodesPageSize = nodesPageSize;
	}

	/**
	 * Obtém o fator de swap dos nós.
	 * @return fator.
	 */
	public double getNodesSwapFactor() {
		return nodesSwapFactor;
	}

	/**
	 * Define o fator de swap dos nós.
	 * @param nodesSwapFactor fator.
	 */
	public void setNodesSwapFactor(double nodesSwapFactor) {
		this.nodesSwapFactor = nodesSwapFactor;
	}

	/**
	 * Obtém o tamanho do buffer usado para armazenar os índices dos itens na memória.
	 * @return tamanho em bytes.
	 */
	public long getIndexBufferSize() {
		return indexBufferSize;
	}

	/**
	 * Define o tamanho do buffer usado para armazenar os índices dos itens na memória.
	 * @param indexBufferSize tamanho em bytes.
	 */
	public void setIndexBufferSize(long indexBufferSize) {
		this.indexBufferSize = indexBufferSize;
	}

	/**
	 * Obtém o tamanho da página do buffer dos íncices.
	 * @return tamanho em bytes.
	 */
	public long getIndexPageSize() {
		return indexPageSize;
	}

	/**
	 * Define o tamanho da página do buffer dos íncices.
	 * @param indexPageSize em bytes.
	 */
	public void setIndexPageSize(long indexPageSize) {
		this.indexPageSize = indexPageSize;
	}

	/**
	 * Obtém o fator de swap dos índices.
	 * @return fator.
	 */
	public double getIndexSwapFactor() {
		return indexSwapFactor;
	}

	/**
	 * Define o fator de swap dos índices.
	 * @param indexSwapFactor fator.
	 */
	public void setIndexSwapFactor(double indexSwapFactor) {
		this.indexSwapFactor = indexSwapFactor;
	}

	/**
	 * Obtém o tamanho do buffer usado para armazenar os itens na memória.
	 * @return tamanho em bytes.
	 */
	public long getDataBufferSize() {
		return dataBufferSize;
	}

	/**
	 * Define o tamanho do buffer usado para armazenar os itens na memória.
	 * @param dataBufferSize tamanho em bytes.
	 */
	public void setDataBufferSize(long dataBufferSize) {
		this.dataBufferSize = dataBufferSize;
	}

	/**
	 * Obtém o tamanho do bloco de dados.
	 * @return tamanho em bytes.
	 */
	public long getDataBlockSize() {
		return dataBlockSize;
	}

	/**
	 * Define o tamanho do bloco de dados.
	 * @param dataBlockSize tamanho em bytes.
	 */
	public void setDataBlockSize(long dataBlockSize) {
		this.dataBlockSize = dataBlockSize;
	}

	/**
	 * Obtém o tamanho da página do buffer dos itens.
	 * @return tamanho em bytes.
	 */
	public long getDataPageSize() {
		return dataPageSize;
	}

	/**
	 * Define o tamanho da página do buffer dos itens.
	 * @param dataPageSize tamanho em bytes.
	 */
	public void setDataPageSize(long dataPageSize) {
		this.dataPageSize = dataPageSize;
	}

	/**
	 * Obtém o fator de swap dos itens.
	 * @return fator.
	 */
	public double getDataSwapFactor() {
		return dataSwapFactor;
	}

	/**
	 * Define o fator de swap dos itens.
	 * @param dataSwapFactor fator.
	 */
	public void setDataSwapFactor(double dataSwapFactor) {
		this.dataSwapFactor = dataSwapFactor;
	}

	/**
	 * Obtém o tamanho máximo que um item pode ter para ser armazenado no cache.
	 * @return tamanho em bytes
	 */
	public long getMaxSizeEntry() {
		return maxSizeEntry;
	}

	/**
	 * Define o tamanho máximo que um item pode ter para ser armazenado no cache.
	 * @param maxSizeEntry tamanho em bytes.
	 */
	public void setMaxSizeEntry(long maxSizeEntry) {
		this.maxSizeEntry = maxSizeEntry;
	}

	/**
	 * Obtém o tamanho máximo que uma chave pode ter.
	 * @return tamanho em bytes.
	 */
	public int getMaxSizeKey() {
		return maxSizeKey;
	}

	/**
	 * Define o tamanho máximo, em bytes, que uma chave pode ter.
	 * @param maxSizeKey tamanho em bytes.
	 */
	public void setMaxSizeKey(int maxSizeKey) {
		this.maxSizeKey = maxSizeKey;
	}

	/**
	 * Obtém a quantidade de threads que irão fazer a troca dos dados da 
	 * memória para o disco, por exemplo.
	 * @return quantidade.
	 */
	public int getSwapperThread() {
		return swapperThread;
	}

	/**
	 * Define a quantidade de threads que irão fazer a troca dos dados da 
	 * memória para o disco, por exemplo.
	 * @param swapperThread quantidade.
	 */
	public void setSwapperThread(int swapperThread) {
		this.swapperThread = swapperThread;
	}

	/**
	 * Obtém a estratégia de troca de dados entre a memória e o disco, por exemplo.
	 * @return estratégia.
	 */
	public SwaperStrategy getSwapper() {
		return swapper;
	}

	/**
	 * Define a estratégia de troca de dados entre a memória e o disco, por exemplo.
	 * @param swapper estratégia.
	 */
	public void setSwapper(SwaperStrategy swapper) {
		this.swapper = swapper;
	}
    
}
