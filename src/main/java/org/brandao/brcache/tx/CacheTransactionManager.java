package org.brandao.brcache.tx;

import java.util.UUID;

public interface CacheTransactionManager {

	void lock(UUID transaction, String key) throws TransactionException;

	void tryLock(UUID transaction, String key, long time) throws TransactionException;

	void unlock(UUID transaction, String key) throws TransactionException;
	
	void commit(UUID transaction) throws TransactionException;
	
	void rollback(UUID transaction) throws TransactionException;
	
	String getTransactionPath();
	
	CacheTransaction begin();
	
	void close(CacheTransaction tx) throws TransactionException;
}
