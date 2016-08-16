package org.brandao.brcache.tx;

import java.io.InputStream;

import org.brandao.brcache.StreamCache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

public interface CacheTransactionHandler 
	extends CacheTransaction{

	void begin() throws TransactionException;
	
	/* métodos de armazenamento */
	
	Object replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException;
	
	boolean replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object oldValue, 
			Object newValue, long maxAliveTime, long time) throws StorageException;
	
	Object putIfAbsent(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException;
	
	void put(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException;
	
    void putStream(CacheTransactionManager manager, StreamCache cache, 
    		String key, long maxAliveTime, InputStream inputData, long time) 
    		throws StorageException;
	
	/* métodos de coleta*/
	
	Object get(CacheTransactionManager manager, StreamCache cache,
			String key, boolean forUpdate, long time) throws RecoverException;
    
    InputStream getStream(CacheTransactionManager manager, StreamCache cache, 
    		String key, boolean forUpdate, long time) throws RecoverException;

    /* métodos de remoção */
    
	boolean remove(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long time) throws StorageException;
	
    boolean remove(CacheTransactionManager manager, StreamCache cache,
    		String key, long time) throws StorageException;
    
}
