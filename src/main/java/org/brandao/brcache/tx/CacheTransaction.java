package org.brandao.brcache.tx;

/**
 * Representa uma unidade de trabalho.
 * 
 * @author Brandao.
 *
 */
public interface CacheTransaction {

	boolean isRolledBack();
	
	boolean isCommited();
	
	void rollback() throws TransactionException;
	
	void commit() throws TransactionException;
	
}
