package org.brandao.brcache;

/**
 * Constantes do cache.
 * 
 * @author Brandao
 *
 */
public interface CacheConstants {

	/**
	 * Pasta onde o servidor irá fazer a troca dos dados quando 
	 * o limite da memória for atingido.
	 */
    public static final String DATA_PATH 			= "data_path";
	
    /**
     * Tamanho do buffer usado para armazenar os nós na memória.
     */
    public static final String NODES_BUFFER_SIZE 	= "nodes_buffer_size";

    /**
     * Tamanho da página do buffer dos nós.
     */
    public static final String NODES_PAGE_SIZE 		= "nodes_page_size";
    
    /**
     * Fator de troca dos nós.
     */
    public static final String NODES_SWAP_FACTOR 	= "nodes_swap_factor";
    
    /**
     * Tamanho do buffer usado para armazenar os índices dos itens na memória.
     */
    public static final String INDEX_BUFFER_SIZE 	= "index_buffer_size";
    
    /**
     * Tamanho da página do buffer dos íncices.
     */
    public static final String INDEX_PAGE_SIZE 		= "index_page_size";
    
    /**
     * Fator de troca dos índices.
     */
    public static final String INDEX_SWAP_FACTOR	= "index_swap_factor";
    
    /**
     * Tamanho do buffer usado para armazenar os itens na memória.
     */
    public static final String DATA_BUFFER_SIZE 	= "data_buffer_size";
    
    /**
     * Tamanho do bloco de dados.
     */
    public static final String DATA_BLOCK_SIZE 		= "data_block_size";
    
    /**
     * Tamanho da página do buffer dos itens.
     */
    public static final String DATA_PAGE_SIZE 		= "data_page_size";
    
    /**
     * Fator de troca dos itens.
     */
    public static final String DATA_SWAP_FACTOR		= "data_swap_factor";
    
    /**
     * Tamanho máximo, em bytes, que um item pode ter para ser armazenado no cache.
     */
    public static final String MAX_SIZE_ENTRY 		= "max_size_entry";
    
    /**
     * Tamanho máximo, em bytes, que uma chave pode ter.
     */
    public static final String MAX_SIZE_KEY 		= "max_size_key";
    
    /**
     * Quantidade de processos que irão fazer a troca dos dados
     * entre a memória e outro dispositivo, por exemplo, disco.
     */
    public static final String SWAPPER_THREAD 		= "swapper_thread";
    
    /**
     * Estratégia de troca.
     */
    public static final String SWAPPER_TYPE 		= "swapper_type";
    
    /**
     * Estratégia de acesso a memória.
     */
    public static final String MEMORY_ACCESS_TYPE 	= "memory_access_type";

}
