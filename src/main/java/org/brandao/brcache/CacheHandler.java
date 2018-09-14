package org.brandao.brcache;

import java.io.InputStream;
import java.io.Serializable;

public interface CacheHandler extends Serializable{
    
    /**
	 * Associa o fluxo de bytes do valor à chave.
	 * @param key chave associada ao fluxo.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o item for substituido. Caso contrário, <code>false</code>
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException;

    /**
     * Substitui o fluxo de bytes associado à chave somente se ele existir.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    boolean replaceStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException;
    
    /**
     * Associa o fluxo de bytes do valor à chave somente se a chave não estiver associada a um valor.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return fluxo associado à chave ou <code>null</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item ou se o item atual 
     * expirar no momento da execução do método.
     */
    InputStream putIfAbsentStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException;
    
    /**
     * Obtém o fluxo de bytes do valor associado à chave.
     * @param key chave associada ao fluxo.
     * @return fluxo de bytes do valor ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
     */
    
    InputStream getStream(String key) throws RecoverException;
    
    /**
     * Remove o valor associado à chave.
     * @param key chave associada ao valor.
     * @return <code>true</code> se o valor for removido. Caso contrário <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
     */
    boolean removeStream(String key) throws StorageException;
    
    /**
     * Verifica se uma chave está associada a um valor.
     * @param key chave associada ao valor.
     * @return <code>true</code> se a chave estiver associada a um valor. Caso contrário, <code>false</code>.
     */
    boolean containsKey(String key);

    /**
     * Obtém o apontamento da chave.
     * @param key chave.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * apontamento.
     */
    
    DataMap getPointer(String key) throws RecoverException;

    void setPointer(String key, DataMap newDta) throws RecoverException;
    
    boolean replacePointer(String key, DataMap originalDta, DataMap newDta) throws RecoverException;
    
    /**
     * Remove os dados do item a partir de seu apontamento e chave.
     * @param key Chave
     * @param data Apontameto.
     */
    void remove(String key, DataMap data);
    
    void releaseSegments(DataMap map);
    
    /**
     * Obtém o fluxo de dados do item a partir de seu apontamento e chave.
     * @param key Chave
     * @param map Apontameto.
     * @return Fluxo de dados.
     * @throws RecoverException Lançada se ocorrer algum problema ao tentar obter os dados.
     */
    InputStream getStream(String key, DataMap map) throws RecoverException;
    
    /**
     * Insere o fluxo de dados do item no cache usando um apontamento.
     * @param map Apontameto.
     * @param inputData Fluxo de dados.
     * @throws StorageException Lançada se ocorrer algum problema ao tentar remover os dados.
     */
    void putData(DataMap map, InputStream inputData) throws StorageException, InterruptedException;

    long getNextModCount();
    
    int getMaxKeySize();
    
	/**
	 * Obtém a configuração do cache.
	 * @return configuração.
	 */
	BRCacheConfig getConfig();
    
    /**
     * Obtém a quantidade de itens recuperados.
     * @return Quantidade de itens recuperados.
     */
    long getCountRead();

    /**
     * Obtém a quantidade de itens armazenados.
     * @return Quantidade de itens armazenados.
     */
    long getCountWrite();

    /**
     * Obtém a quantidade de itens removidos.
     * @return Quantidade de itens removidos.
     */
    long getCountRemoved();

    /**
     * Obtém a quantidade de bytes recuperados.
     * @return Quantidade de bytes recuperados.
     */
    long getCountReadData();
    
    /**
     * Obtém a quantidade de bytes armazenados.
     * @return Quantidade de bytes armazenados.
     */
    long getCountWriteData();

    /**
     * Obtém a quantidade de bytes removidos.
     * @return Quantidade de bytes removidos.
     */
    long getCountRemovedData();
    
    /**
     * Verifica se os arquivos contidos na pasta de dados serão 
     * destruidos junto com essa instância. 
     * @return <code>true</code> para destruir todos os arquivos. 
     * Caso contrário, <code>false</code>.
     */
    boolean isDeleteOnExit();

    /**
     * Define que os arquivos contidos na pasta de dados sejam 
     * destruidos junto com essa instância. 
     * @param deleteOnExit <code>true</code> para destruir todos os arquivos. 
     * Caso contrário, <code>false</code>.
     */
	void setDeleteOnExit(boolean deleteOnExit);

	/**
	 * Obtém a quantidade de itens no cache.
	 * @return Quantidade.
	 */
	long size();
	
	/**
	 * Verifica se o cache está vazio.
	 * @return <code>true</code> se o cache estiver vazio.
	 */
	boolean isEmpty();
	
	/**
	 * Remove todas as entradas contidas no cache.
	 */
	void clear();
	
	/**
	 * Destrói os dados contidos nesta instância. Deve ser executado se {@link #isDeleteOnExit()} 
	 * for <code>false</code>.
	 */
	void destroy();
	
}
