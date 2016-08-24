package org.brandao.brcache.tx;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.StreamCache;

public interface CacheTransactionManager {

	void lock(UUID transaction, String key) throws TransactionException;

	void tryLock(UUID transaction, String key, long time, TimeUnit unit) throws TransactionException;

	void unlock(UUID transaction, String key) throws TransactionException;
	
	void commit(UUID transaction) throws TransactionException;
	
	void rollback(UUID transaction) throws TransactionException;
	
	String getTransactionPath();
	
	CacheTransactionHandler begin(StreamCache cache);
	
	CacheTransactionHandler getCurrrent();

	CacheTransactionHandler getCurrrent(boolean required);
	
	void close(CacheTransactionHandler tx) throws TransactionException;
}
