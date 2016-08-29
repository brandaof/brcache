package org.brandao.brcache.tx;

import org.brandao.brcache.CacheError;
import org.brandao.brcache.CacheException;

/**
 * Lançada se ocorrer alguma falha ao processar uma transação.
 * 
 * @author Brandao
 *
 */
public class TransactionException 
	extends CacheException{

	private static final long serialVersionUID = 7884198916723934702L;

	public TransactionException(CacheError error, Object... params) {
		super(error, params);
	}

	public TransactionException(Throwable thrwbl, CacheError error,
			Object... params) {
		super(thrwbl, error, params);
	}

}
