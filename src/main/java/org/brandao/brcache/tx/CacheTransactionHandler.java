package org.brandao.brcache.tx;

import java.io.InputStream;
import java.io.Serializable;

import org.brandao.brcache.CacheHandler;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

interface CacheTransactionHandler extends CacheTransaction{

	Serializable getId();
	
	void begin();
	
	void close();
	
	/* métodos de armazenamento */
	
	boolean replaceStream(CacheTransactionManager manager, CacheHandler cache,
			String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException;
	
	InputStream putIfAbsentStream(CacheTransactionManager manager, CacheHandler cache,
			String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException;
    		
    boolean putStream(CacheTransactionManager manager, CacheHandler cache, 
    		String key, InputStream inputData, long timeToLive, long timeToIdle) 
    		throws StorageException;
	
	/* métodos de coleta*/
	
    InputStream getStream(CacheTransactionManager manager, CacheHandler cache, 
    		String key, boolean forUpdate) throws RecoverException;

    /* métodos de remoção */
    
    boolean remove(CacheTransactionManager manager, CacheHandler cache,
    		String key) throws StorageException;
    
}
