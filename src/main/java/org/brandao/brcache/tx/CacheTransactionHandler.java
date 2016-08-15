package org.brandao.brcache.tx;

import java.io.InputStream;

import org.brandao.brcache.StreamCache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

public interface CacheTransactionHandler 
	extends CacheTransaction{

	void begin() throws TransactionException;
	
    void putObject(CacheTransactionManager manager, StreamCache cache, 
    		String key, long maxAliveTime, Object item) throws StorageException;

    Object getObject(CacheTransactionManager manager, StreamCache cache, 
    		String key, boolean forUpdate, long time) throws RecoverException;

    void put(CacheTransactionManager manager, StreamCache cache, 
    		String key, long maxAliveTime, InputStream inputData, long time) 
    		throws StorageException;
    
    InputStream get(CacheTransactionManager manager, StreamCache cache, 
    		String key, boolean forUpdate, long time) throws RecoverException;
    
    boolean remove(CacheTransactionManager manager, StreamCache cache,
    		String key, long time) throws RecoverException;
	
}
