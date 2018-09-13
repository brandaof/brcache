package org.brandao.brcache.tx;

import java.io.Serializable;

interface CacheTransactionHandler extends CacheTransaction, TransactionCacheHandler{

	Serializable getId();
	
	void begin();
	
	void close();
    
}
