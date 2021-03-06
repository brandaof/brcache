package org.brandao.brcache.tx;

import java.io.InputStream;
import java.io.Serializable;

import org.brandao.brcache.BasicCache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

interface CacheTransactionHandler 
	extends CacheTransaction{

	Serializable getId();
	
	void begin();
	
	void close();
	
	/* métodos de armazenamento */
	
	Object replace(CacheTransactionManager manager, BasicCache cache,
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException;
	
	boolean replaceStream(CacheTransactionManager manager, BasicCache cache,
			String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException;
	
	boolean replace(CacheTransactionManager manager, BasicCache cache,
			String key, Object oldValue, Object newValue, long timeToLive, long timeToIdle) throws StorageException;
	
	Object putIfAbsent(CacheTransactionManager manager, BasicCache cache,
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException;
	
	InputStream putIfAbsentStream(CacheTransactionManager manager, BasicCache cache,
			String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException;
    		
	boolean put(CacheTransactionManager manager, BasicCache cache,
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException;
	
    boolean putStream(CacheTransactionManager manager, BasicCache cache, 
    		String key, InputStream inputData, long timeToLive, long timeToIdle) 
    		throws StorageException;
	
	/* métodos de coleta*/
	
	Object get(CacheTransactionManager manager, BasicCache cache,
			String key, boolean forUpdate) throws RecoverException;
    
    InputStream getStream(CacheTransactionManager manager, BasicCache cache, 
    		String key, boolean forUpdate) throws RecoverException;

    /* métodos de remoção */
    
	boolean remove(CacheTransactionManager manager, BasicCache cache,
			String key, Object value) throws StorageException;
	
    boolean remove(CacheTransactionManager manager, BasicCache cache,
    		String key) throws StorageException;
    
}
