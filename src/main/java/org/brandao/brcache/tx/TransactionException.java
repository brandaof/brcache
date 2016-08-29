package org.brandao.brcache.tx;

import org.brandao.brcache.CacheError;
import org.brandao.brcache.CacheException;

/**
 * 
 * @author cliente
 *
 */
public class TransactionException 
	extends CacheException{

	private static final long serialVersionUID = 7884198916723934702L;

	public TransactionException(CacheError error, Object... params) {
		super(error, params);
		// TODO Auto-generated constructor stub
	}

	public TransactionException(Throwable thrwbl, CacheError error,
			Object... params) {
		super(thrwbl, error, params);
		// TODO Auto-generated constructor stub
	}

}
