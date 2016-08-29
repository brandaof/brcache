package org.brandao.brcache.tx;

/**
 * Representa uma transação em um cache.
 * @author Brandao.
 *
 */
public interface CacheTransaction {

	/**
	 * Verifica se todas as operações contidas na transação foram desfeitas.
	 * @return <code>true</code> se foram desfeitas. Caso contrário, <code>false</code>
	 */
	boolean isRolledBack();
	
	/**
	 * Verifica se todas as operações contidas na transação foram confirmadas.
	 * @return <code>true</code> se foram confirmadas. Caso contrário, <code>false</code>
	 */
	boolean isCommited();
	
	/**
	 * Desfaz todas as operações contidas na transação.
	 */
	void rollback() throws TransactionException;
	
	/**
	 * Confirma todas as operações contidas na transação.
	 */
	void commit() throws TransactionException;
	
}
