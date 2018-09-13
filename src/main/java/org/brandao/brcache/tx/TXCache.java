/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.tx;

import java.io.Serializable;

import org.brandao.brcache.CacheHandler;
import org.brandao.brcache.ConcurrentCache;

/**
 * Provê suporte transacional a um cache.
 * 
 * <pre>
 * ex:
 * 
 *    Cache cache = ...;
 *    TXCache txCache = cache.getTXCache();
 *    CacheTransaction tx = txCache.beginTransaction();
 *    
 *    try{
 *        cache.put("chave1", objeto1, 0, 0);
 *        cache.remove("chave2");
 *        cache.putIfAbsent("chave3", objeto3, 0, 0);
 *        tx.commit();
 *    }
 *    catch(Throwable e){
 *        tx.rollback();
 *    }
 * </pre>
 * 
 * @author Brandao
 */
public class TXCache extends ConcurrentCache implements Serializable{
    
	private static final long serialVersionUID = 4338728178626320357L;
	
	private CacheTransactionManager transactionManager;

    /**
     * Cria um cache transacional especificando o gestor transacional.
     * @param cache cache não transacional.
     * @param transactionManager gestor transacional.
     */
    public TXCache(CacheHandler cache, CacheTransactionManager transactionManager){
    	super(new TXCacheHandler(cache, transactionManager));
    	this.transactionManager = transactionManager;
    	super.setDeleteOnExit(false);
    }
    
    /**
     * Obtém o gestor transacional.
     * @return gestor transacional.
     */
    public CacheTransactionManager getTransactionManager(){
    	return transactionManager;
    }
    
    /**
     * Obtém o tempo limite de uma operação.
     * @return tempo em milisegundos.
     */
    public long getTransactionTimeout() {
		return transactionManager.getTimeout();
	}

    /**
     * Define o tempo limite de uma operação.
     * @param transactionTimeout tempo em milisegundos.
     */
	public void setTransactionTimeout(long transactionTimeout) {
		transactionManager.setTimeout(transactionTimeout);
	}

	/**
	 * Inicia uma transação.
	 * @return transação.
	 */
	public CacheTransaction beginTransaction(){
    	return transactionManager.begin(cacheHandler);
    }
    
}
