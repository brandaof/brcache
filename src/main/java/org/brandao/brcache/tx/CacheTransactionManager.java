package org.brandao.brcache.tx;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.BRCacheConfig;
import org.brandao.brcache.BasicCache;

/**
 * Gestor das transações de um cache.
 * 
 * @author Brandao.
 *
 */
public interface CacheTransactionManager {

	void lock(Serializable transaction, String key) throws TransactionException;

	void tryLock(Serializable transaction, String key, long time, TimeUnit unit) throws TransactionException;

	void unlock(Serializable transaction, String key) throws TransactionException;
	
	void commit(Serializable transaction) throws TransactionException;
	
	void rollback(Serializable transaction) throws TransactionException;
	
	void setPath(String value);
	
	String getPath();

	void setTimeout(long value);
	
	long getTimeout();
	
	CacheTransactionHandler begin(BasicCache cache);
	
	CacheTransactionHandler getCurrrent();

	CacheTransactionHandler getCurrrent(boolean required);
	
	void close(CacheTransactionHandler tx) throws TransactionException;
}
