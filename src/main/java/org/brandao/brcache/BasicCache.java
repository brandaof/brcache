package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.TXCache;

/**
 * Provê as operações básicas de um cache.
 * 
 * @author Brandao.
 *
 */
public class BasicCache extends AbstractCache {

	private static final long serialVersionUID = -3101078990483084845L;
	
	protected BRCacheConfig config;
	
    public BasicCache(BRCacheConfig config){
    	super(
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
			config.getEntityFileManager());
    	this.config = config;
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

	public boolean replace(String key, Object oldValue, Object newValue,
			long timeToLive, long timeToIdle) throws StorageException {
		throw new UnsupportedOperationException();
	}

	public boolean remove(String key, Object value) throws StorageException {
		throw new UnsupportedOperationException();
	}

	public long size() {
		return super.getCountRemoved() - super.getCountWrite();
	}

	public boolean isEmpty() {
		return this.size() == 0;
	}
    
}
