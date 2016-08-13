package org.brandao.brcache.tx;

public interface CacheTransaction {

	boolean isRolledBack();
	
	boolean isCommited();
	
	void rollback() throws TransactionException;
	
	void commit() throws TransactionException;
	
}
