package org.brandao.brcache.tx;

public interface CacheTransactionManager {

	void lock(byte[] transaction, String key) throws TransactionException;

	void tryLock(byte[] transaction, String key, long time) throws TransactionException;

	void unlock(byte[] transaction, String key) throws TransactionException;
	
	void commit(byte[] transaction) throws TransactionException;
	
	void rollback(byte[] transaction) throws TransactionException;
	
	String getTransactionPath();
	
	CacheTransaction begin();
	
	void close(CacheTransaction tx) throws TransactionException;
}
