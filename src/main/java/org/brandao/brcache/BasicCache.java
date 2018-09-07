package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.brandao.brcache.memory.Memory;
import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.TXCache;
import org.brandao.entityfilemanager.EntityFileManager;

/**
 * Provê as operações básicas de um cache.
 * 
 * @author Brandao.
 *
 */
public class BasicCache extends AbstractCache {

	private static final long serialVersionUID = -3101078990483084845L;
	
	protected BRCacheConfig config;
	
    /**
     * Cria um novo cache.
     * 
     */
    public BasicCache(EntityFileManager efm){
    	this(new BRCacheConfig(new Configuration()), efm);
    }
    
    public BasicCache(BRCacheConfig config, EntityFileManager efm){
    	this(
			"default",
			config.getNodesBufferSize(), 
			config.getNodesPageSize(), 
			config.getNodesSwapFactor(),
			
			config.getIndexBufferSize(), 
			config.getIndexPageSize(), 
			config.getIndexSwapFactor(),
			
			config.getDataBufferSize(), 
			config.getDataPageSize(), 
			config.getDataBlockSize(),
			config.getDataSwapFactor(),
			
			config.getMaxSizeEntry(), 
			config.getMaxSizeKey(), 
			config.getSwapperThread(), 
			config.getMemory(),
			efm);
    	this.config = config;
    }
    
    /**
     * Cria um novo cache.
     * 
     * @param name Nome do cache.
     * @param nodeBufferSize Tamanho do buffer, em bytes, onde os nós ficarão armazenados. 
     * @param nodePageSize Tamanho da página, em bytes, do buffer de nós.
     * @param nodeSwapFactor Fator de permuta dos nós.
     * @param indexBufferSize Tamanho do buffer, em bytes, onde os índices ficarão armazenados.
     * @param indexPageSize Tamanho da página, em bytes, do buffer de índices.
     * @param indexSwapFactor Fator de permuta dos índices.
     * @param dataBufferSize Tamanho do buffer, em bytes, onde os dados ficarão armazenados. 
     * @param dataPageSize Tamanho da página, em bytes, do buffer de dados.
     * @param blockSize Tamanho do bloco, em bytes.
     * @param dataSwapFactor Fator de permuta dos dados.
     * @param maxSizeEntry Tamanho máximo de uma entrada no cache.
     * @param maxSizeKey Tamanho máximo de uma chave.
     * @param quantitySwaperThread Quantidade de processos usados para fazer a permuta.
     * @param memory Acesso à memória.
     * @param efm Sistema de arquivos usado pelo cache.
     */
    public BasicCache(
    		String name,
    		long nodeBufferSize,
    		long nodePageSize,
    		double nodeSwapFactor,
    		
    		long indexBufferSize,
    		long indexPageSize,
    		double indexSwapFactor,
    		
    		long dataBufferSize,
    		long dataPageSize,
    		long blockSize,
    		double dataSwapFactor,
    		
    		long maxSizeEntry,
    		int maxSizeKey,
            int quantitySwaperThread,
            Memory memory,
            EntityFileManager efm
    		){
    	this.init(name, nodeBufferSize, nodePageSize, nodeSwapFactor, indexBufferSize, 
    			indexPageSize, indexSwapFactor, dataBufferSize, dataPageSize, blockSize, 
    			dataSwapFactor, maxSizeEntry, maxSizeKey, quantitySwaperThread, memory, efm);
    }
    
    /**
     * Obtém o cache com suporte transacional.
     * @return cache.
     */
    public TXCache getTXCache(){
    	return new TXCache(this);
    }

    /**
     * Obtém o cache com suporte transacional com um gestor transacional específicos.
     * @param txManager gestor transacional.
     * @return cache com suporte transacional.
     */
    public TXCache getTXCache(CacheTransactionManager txManager){
    	return new TXCache(this, txManager);
    }
    
	/* métodos de armazenamento */
    
    /**
     * Substitui o valor associado à chave somente se ele existir.
     * @param key chave associada ao valor.
     * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
	public boolean replace(String key, Object value, 
			long timeToLive, long timeToIdle) throws StorageException {
		
		ByteArrayOutputStream bout;
		
		try{
			bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
		
		try{
			return super.replaceStream(key, new ByteArrayInputStream(bout.toByteArray()), 
					timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(RecoverException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
    /**
     * Substitui o fluxo de bytes associado à chave somente se ele existir.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public boolean replaceStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return super.replaceStream(key, inputData, timeToLive, timeToIdle);
    }
	
	/**
	 * Associa o valor à chave somente se a chave não estiver associada a um valor.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
	 * @return valor anterior associado à chave.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item ou se o item atual 
     * expirar no momento da execução do método.
	 */
    public Object putIfAbsent(String key, Object value, 
    		long timeToLive, long timeToIdle) throws StorageException{
		ByteArrayOutputStream bout;
		
		try{
			bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}

		try{
			InputStream in =
				super.putIfAbsentStream(key, new ByteArrayInputStream(bout.toByteArray()),
						timeToLive, timeToIdle);
			
			if(in != null){
				ObjectInputStream oin = new ObjectInputStream(in);
				return oin.readObject();
			}
			else
				return null;
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
    }
    
    /**
     * Associa o fluxo de bytes do valor à chave somente se a chave não estiver associada a um valor.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return fluxo associado à chave ou <code>null</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public InputStream putIfAbsentStream(String key, InputStream inputData,
    		long timeToLive, long timeToIdle) throws StorageException{
    	return super.putIfAbsentStream(key, inputData, timeToLive, timeToIdle);
    }
    
	/**
	 * Associa o valor à chave.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @return <code>true</code> se o item for substituido. Caso contrário, <code>false</code>
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
	 */
	public boolean put(String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		ByteArrayOutputStream bout;
		
		try{
			bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
		
		try{
			return this.putStream(key, new ByteArrayInputStream(bout.toByteArray()), 
					timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
    
    /**
	 * Associa o fluxo de bytes do valor à chave.
	 * @param key chave associada ao fluxo.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o item for substituido. Caso contrário, <code>false</code>
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return super.putStream(key, inputData, timeToLive, timeToIdle);
    }
	
    /* métodos de coleta */
    
	/**
	 * Obtém o valor associado à chave.
	 * @param key chave associada ao valor.
     * @return valor associado à chave ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
	 */
	public Object get(String key) throws RecoverException {
		try{
			InputStream in = super.getStream(key);
			if(in != null){
				ObjectInputStream oin = new ObjectInputStream(in);
				return oin.readObject();
			}
			else
				return null;
		}
		catch(RecoverException e){
			throw e;
		}	
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
	}
	
    /**
     * Obtém o fluxo de bytes do valor associado à chave.
     * @param key chave associada ao fluxo.
     * @return fluxo de bytes do valor ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
     */
    public InputStream getStream(String key) throws RecoverException {
    	return super.getStream(key);
    }
    
    /* métodos de remoção */
    
    /**
     * Remove o valor associado à chave.
     * @param key chave associada ao valor.
     * @return <code>true</code> se o valor for removido. Caso contrário <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
     */
    public boolean remove(String key) throws StorageException{
    	return super.removeStream(key);
    }

	/**
	 * Obtém a configuração do cache.
	 * @return configuração.
	 */
	public BRCacheConfig getConfig() {
		return config;
	}
    
}
