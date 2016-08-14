package org.brandao.brcache.tx;

import java.io.InputStream;

import org.brandao.brcache.Cache;

public interface CacheTransactionHandler 
	extends CacheTransaction{

	void begin() throws TransactionException;
	
    void putObject(CacheTransactionManager manager, Cache cache, 
    		String key, long maxAliveTime, Object item) throws TransactionException;

    Object getObject(CacheTransactionManager manager, Cache cache, 
    		String key, boolean forUpdate, long time) throws TransactionException;

    void put(CacheTransactionManager manager, Cache cache, 
    		String key, long maxAliveTime, InputStream inputData) 
    		throws TransactionException;
    
    InputStream get(CacheTransactionManager manager, Cache cache, 
    		String key, boolean forUpdate, long time) throws TransactionException;
    
    boolean remove(CacheTransactionManager manager, Cache cache,
    		String key) throws TransactionException;
	
}
