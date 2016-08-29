package org.brandao.brcache.tx;

import java.util.UUID;
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

	void lock(UUID transaction, String key) throws TransactionException;

	void tryLock(UUID transaction, String key, long time, TimeUnit unit) throws TransactionException;

	void unlock(UUID transaction, String key) throws TransactionException;
	
	void commit(UUID transaction) throws TransactionException;
	
	void rollback(UUID transaction) throws TransactionException;
	
	String getTransactionPath();
	
	void setConfiguration(BRCacheConfig config);
	
	CacheTransactionHandler begin(BasicCache cache);
	
	CacheTransactionHandler getCurrrent();

	CacheTransactionHandler getCurrrent(boolean required);
	
	void close(CacheTransactionHandler tx) throws TransactionException;
}
