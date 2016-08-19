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

package org.brandao.brcache;

import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.brandao.brcache.tx.CacheTransaction;
import org.brandao.brcache.tx.CacheTransactionHandler;
import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.CacheTransactionManagerImp;
import org.brandao.brcache.tx.TransactionException;

/**
 * Provê suporte transacional a um cache.
 * 
 * ex:
 * 
 *    Cache cache = new Cache();
 *    TXCache txCache = cache.getTXCache();
 *    CacheTransaction tx = txCache.beginTransaction();
 *    try{
 *        cache.put("chave1", objeto1);
 *        cache.remove("chave2");
 *        cache.putIfAbsent("chave3", objeto3);
 *        tx.commit();
 *    }
 *    catch(Throwable e){
 *        tx.rollback();
 *    }
 *    
 * @author Brandao
 */
public class TXCache 
	extends Cache
	implements Serializable{
    
	private static final long serialVersionUID = 1L;

	private static final long TIME_OUT = 5*60*1000;
	
	private static final Method replace;

	private static final Method replaceExact;

	private static final Method putIfAbsent;

	private static final Method put;

	private static final Method putStream;

	private static final Method get;

	private static final Method getStream;

	private static final Method remove;

	private static final Method removeExact;
	
	static{
		try{
			replace = CacheTransactionHandler.class.getDeclaredMethod(
					"replace",	CacheTransactionManager.class, 
					StreamCache.class, String.class, Object.class, long.class, long.class);

			replaceExact = CacheTransactionHandler.class.getDeclaredMethod(
					"replace", CacheTransactionManager.class, StreamCache.class,
					String.class, Object.class, 
					Object.class, long.class, long.class);

			putIfAbsent = CacheTransactionHandler.class.getDeclaredMethod(
					"putIfAbsent", CacheTransactionManager.class, StreamCache.class,
					String.class, Object.class, long.class, long.class);

			put = CacheTransactionHandler.class.getDeclaredMethod(
					"put", CacheTransactionManager.class, StreamCache.class,
					String.class, Object.class, long.class, long.class);

			putStream = CacheTransactionHandler.class.getDeclaredMethod(
					"putStream", CacheTransactionManager.class, StreamCache.class, 
		    		String.class, long.class, InputStream.class, long.class);

			get = CacheTransactionHandler.class.getDeclaredMethod(
					"get", CacheTransactionManager.class, StreamCache.class,
					String.class, boolean.class, long.class);

			getStream = CacheTransactionHandler.class.getDeclaredMethod(
					"getStream", CacheTransactionManager.class, StreamCache.class, 
		    		String.class, boolean.class, long.class);

			removeExact = CacheTransactionHandler.class.getDeclaredMethod(
					"remove", CacheTransactionManager.class, StreamCache.class,
					String.class, Object.class, long.class);

			remove = CacheTransactionHandler.class.getDeclaredMethod(
					"remove", CacheTransactionManager.class, StreamCache.class,
		    		String.class, long.class);

			replace.setAccessible(true);
			replaceExact.setAccessible(true);
			putIfAbsent.setAccessible(true);
			put.setAccessible(true);
			putStream.setAccessible(true);
			get.setAccessible(true);
			getStream.setAccessible(true);
			removeExact.setAccessible(true);
			remove.setAccessible(true);
		}
		catch(Throwable e){
			throw new ExceptionInInitializerError(e);
		}
	}
	
	private Cache cache;
	
	private CacheTransactionManager transactionManager;

	private long transactionTimeout;

	/**
	 * Cria um cache transacional a partir de um cache.
	 * @param cache cache não transacional.
	 */
    public TXCache(Cache cache){
    	this(cache, new CacheTransactionManagerImp() , TIME_OUT);
    }
	
    /**
     * Cria um cache transacional especificando o gestor transacional.
     * @param cache cache não transacional.
     * @param transactionManager gestor transacional.
     */
    public TXCache(Cache cache, CacheTransactionManager transactionManager){
    	this(cache, transactionManager, TIME_OUT);
    }

    /**
     * Cria um cache transacional especificando o gestor transacional e o tempo limite.
     * @param cache cache não transacional.
     * @param transactionManager gestor transacional.
     * @param timeout tempo limite. É o tempo máximo que se espera em ms para concluir uma operação
     * no cache.
     */
    public TXCache(Cache cache, CacheTransactionManager transactionManager, long timeout){
    	this.cache = cache;
    	this.transactionManager = transactionManager;
    	this.transactionTimeout = timeout;
    }
    
    /**
     * Obté o tempo limite de uma operação.
     * @return tempo.
     */
    public long getTransactionTimeout() {
		return transactionTimeout;
	}

    /**
     * Define o tempo limite de uma operação.
     * @param transactionTimeout tempo.
     */
	public void setTransactionTimeout(long transactionTimeout) {
		this.transactionTimeout = transactionTimeout;
	}

	/**
	 * Inicia uma transação.
	 * @return transação.
	 */
	public CacheTransaction beginTransaction(){
    	return this.transactionManager.begin(this.cache);
    }

	/* métodos de armazenamento */
	
    /**
     * Substitui o valor associado à chave somente se ele existir.
     * @param key chave associado ao valor.
     * @param value valor para ser associado à chave.
     * @param maxAliveTime tempo máximo de vida do valor no cache.
     * @return o valor anterior associado à chave.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
     */
	public Object replace(
			String key, Object value, long maxAliveTime) throws StorageException {
		
		try{
			return this.executeMethodInTX(replace, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, value, maxAliveTime, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	/**
	 * Substitui o valor associado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associado ao valor.
	 * @param oldValue valor esperado associado à chave.
	 * @param newValue valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
	 * @return <code>verdadeiro</code> se o valor for substituido. Caso contrário, <code>falso</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
	 */
	public boolean replace(
			String key, Object oldValue, 
			Object newValue, long maxAliveTime) throws StorageException {
		
		try{
			return (Boolean)this.executeMethodInTX(replaceExact, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, oldValue, 
					newValue, maxAliveTime, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}

	}
	
	/**
	 * Associa o valor a chave somente se a chave não estiver associada a um valor.
	 * @param key chave associado ao valor.
	 * @param value valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
	 * @return valor anterior associado à chave.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
	 */
	public Object putIfAbsent(
			String key, Object value, long maxAliveTime) throws StorageException {
		
		try{
			return this.executeMethodInTX(putIfAbsent, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, value, maxAliveTime, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	/**
	 * Associa uma valor a uma chave.
	 * @param key chave associado ao valor.
	 * @param value valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
	 */
	public void put(
			String key, Object value, long maxAliveTime) throws StorageException {
		try{
			this.executeMethodInTX(put, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, value, maxAliveTime, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
    /**
     * Inclui ou sobrescreve um item no cache.
     * @param key Identificação do item no cache.
     * @param maxAliveTime Tempo máximo em milesegundos que o item ficará no cache.
     * @param inputData Fluxo de dados que representa o item.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
     */
    public void putStream(
    		String key, long maxAliveTime, InputStream inputData) 
    		throws StorageException {
		try{
			this.executeMethodInTX(putStream, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache, 
		    		key, maxAliveTime, inputData, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
    }
	
	/* métodos de coleta*/
	
	/**
	 * Obtém o valor associado à chave.
	 * @param key chave associado ao valor.
	 * @return valor para ser associado à chave.
	 * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * item do cache.
	 */
	public Object get(
			String key) throws RecoverException {
		return this.get(key, false);
	}
    
	public Object get(
			String key, boolean forUpdate) throws RecoverException {
		try{
			return this.executeMethodInTX(get, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, forUpdate, this.transactionTimeout);
		}
		catch(RecoverException e){
			throw e;
		}
		catch(Throwable e){
			throw new RecoverException(e);
		}
	}
    
    /**
     * Recupera um item do cache.
     * @param key Identificação do item no cache.
     * @return Fluxo de dados que representa o item ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * item do cache.
     */
    public InputStream getStream( 
    		String key) throws RecoverException {
    	return this.getStream(key, false);
    }
    
    public InputStream getStream( 
    		String key, boolean forUpdate) throws RecoverException {
    	
		try{
			return (InputStream)this.executeMethodInTX(getStream, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache, 
		    		key, forUpdate, this.transactionTimeout);
		}
		catch(RecoverException e){
			throw e;
		}
		catch(Throwable e){
			throw new RecoverException(e);
		}
    }

    /* métodos de remoção */
    
	/**
	 * Remove o valor assoiado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associado ao valor.
	 * @return valor para ser associado à chave.
	 * @return <code>verdadeiro</code> se o valor for removido. Caso contrário, <code>falso</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
	 */
	public boolean remove(
			String key, Object value) throws StorageException {
		
		try{
			return (Boolean)this.executeMethodInTX(removeExact, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, value, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	/**
	 * Remove o valor associado à chave.
	 * @param key chave associado ao valor.
	 * @return <code>verdadeiro</code> se o valor for removido. Caso contrário, <code>falso</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
	 */
    public boolean remove(
    		String key) throws StorageException{       
		try{
			return (Boolean)this.executeMethodInTX(remove, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
		    		key, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
    }	
    
    /**
     * Obtém a quantidade de itens recuperados.
     * 
     * @return Quantidade de itens recuperados.
     */
    public long getCountRead(){
        return this.cache.getCountRead();
    }

    /**
     * Obtém a quantidade de itens armazenados.
     * 
     * @return Quantidade de itens armazenados.
     */
    public long getCountWrite(){
        return this.cache.getCountWrite();
    }

    /**
     * Obtém a quantidade de itens removidos.
     * 
     * @return Quantidade de itens removidos.
     */
    public long getCountRemoved() {
		return this.cache.getCountRemoved();
	}

    /**
     * Obtém a quantidade de bytes recuperados.
     * 
     * @return Quantidade de bytes recuperados.
     */
    public long getCountReadData() {
        return this.cache.getCountReadData();
    }
    
    /**
     * Obtém a quantidade de bytes armazenados.
     * 
     * @return Quantidade de bytes armazenados.
     */
    public long getCountWriteData() {
        return this.cache.getCountWriteData();
    }

    /**
     * Obtém a quantidade de bytes removidos.
     * 
     * @return Quantidade de bytes removidos.
     */
    public long getCountRemovedData() {
        return this.cache.getCountRemovedData();
    }

    /* métodos internos */
    
    private Object executeMethodInTX(Method method, 
    		CacheTransactionHandler currentTx, Object ... params) throws Throwable{
    	
    	CacheTransactionHandler tx = 
    			currentTx == null? 
    					this.transactionManager.begin(this.cache) : 
    					currentTx;
    	
    	try{
    		Object r = method.invoke(this, params);
			if(currentTx == null){
				tx.commit();
			}
			return r;
    	}
    	catch(IllegalAccessException e){
			throw new StorageException("bug!", e); 
    	}
    	catch(IllegalArgumentException e){
			throw new StorageException("bug!", e); 
    	}
    	catch(InvocationTargetException e){
    		Throwable ex = e.getTargetException();
    		
    		try{
    			if(currentTx == null){
    				tx.rollback();
    			}
    		}
			catch(Throwable x){
				throw new StorageException(
						"bug: exception not recognized (rollback fail): " + x.toString(), ex); 
			}
    		
    		if(ex instanceof StorageException || ex instanceof RecoverException){
        		throw ex;
    		}
    		else{
    			throw new StorageException("bug: exception not recognized: ", ex);
    		}
    		
    	}
    	finally{
    		try{
    			this.transactionManager.close(tx);
    		}
    		catch(TransactionException e){
    			throw new StorageException(e);    			
    		}
    	}
    	
    }
    
}
