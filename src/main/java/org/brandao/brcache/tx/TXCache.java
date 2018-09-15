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

import java.io.InputStream;
import java.io.Serializable;

import org.brandao.brcache.Cache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

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
public interface TXCache extends Cache, Serializable{
    
	/**
	 * Obtém o valor associado à chave bloqueando-o ou não.
	 * @param key chave associada ao valor.
	 * @param forUpdate <code>true</code> bloqueia o item.
     * @return valor associado à chave ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
	 */
	InputStream getStream(String key, boolean forUpdate) throws RecoverException;
    
	/**
	 * Obtém o valor associado à chave bloqueando-o ou não.
	 * @param key chave associada ao valor.
	 * @param forUpdate <code>true</code> bloqueia o item.
     * @return valor associado à chave ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
	 */
	Object get(String key, boolean forUpdate) throws RecoverException;

	boolean replace(String key, Object oldValue, Object newValue,
			long timeToLive, long timeToIdle) throws StorageException;

    /**
     * Obtém o gestor transacional.
     * @return gestor transacional.
     */
    CacheTransactionManager getTransactionManager();
    
    /**
     * Obtém o tempo limite de uma operação.
     * @return tempo em milisegundos.
     */
    long getTransactionTimeout();

    /**
     * Define o tempo limite de uma operação.
     * @param transactionTimeout tempo em milisegundos.
     */
	void setTransactionTimeout(long transactionTimeout);

	/**
	 * Inicia uma transação.
	 * @return transação.
	 */
	CacheTransaction beginTransaction();
    
}
