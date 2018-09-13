package org.brandao.brcache.tx;

import java.io.InputStream;

import org.brandao.brcache.CacheHandler;
import org.brandao.brcache.RecoverException;

public interface TransactionCacheHandler extends CacheHandler{

	InputStream getStream(String key, boolean forUpdate) throws RecoverException;
	
}
