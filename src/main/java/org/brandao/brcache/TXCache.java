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
import org.brandao.brcache.tx.TransactionException;

/**
 * Cache Transacional
 * 
 * @author Brandao
 */
public class TXCache 
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

    public TXCache(Cache cache){
    	this(cache, null, TIME_OUT);
    }
	
    public TXCache(Cache cache, CacheTransactionManager transactionManager){
    	this(cache, transactionManager, TIME_OUT);
    }

    public TXCache(Cache cache, CacheTransactionManager transactionManager, long timeout){
    	this.cache = cache;
    	this.transactionManager = transactionManager;
    	this.transactionTimeout = timeout;
    }
    
    public long getTransactionTimeout() {
		return transactionTimeout;
	}

	public void setTransactionTimeout(long transactionTimeout) {
		this.transactionTimeout = transactionTimeout;
	}

	public CacheTransaction beginTransaction(){
    	return this.transactionManager.begin(this.cache);
    }

	/* métodos de armazenamento */
	
	public Object replace(
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		
		try{
			return this.executeMethodInTX(replace, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, value, maxAliveTime, time);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	public boolean replace(
			String key, Object oldValue, 
			Object newValue, long maxAliveTime, long time) throws StorageException {
		
		try{
			return (Boolean)this.executeMethodInTX(replaceExact, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, oldValue, 
					newValue, maxAliveTime, time);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}

	}
	
	public Object putIfAbsent(
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		
		try{
			return this.executeMethodInTX(putIfAbsent, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, value, maxAliveTime, time);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	public void put(
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		try{
			this.executeMethodInTX(put, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, value, maxAliveTime, time);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
    public void putStream(
    		String key, long maxAliveTime, InputStream inputData, long time) 
    		throws StorageException {
		try{
			this.executeMethodInTX(putStream, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache, 
		    		key, maxAliveTime, inputData, time);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
    }
	
	/* métodos de coleta*/
	
	public Object get(
			String key, boolean forUpdate, long time) throws RecoverException {
		try{
			return this.executeMethodInTX(get, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, forUpdate, time);
		}
		catch(RecoverException e){
			throw e;
		}
		catch(Throwable e){
			throw new RecoverException(e);
		}
	}
    
    public InputStream getStream( 
    		String key, boolean forUpdate, long time) throws RecoverException {
    	
		try{
			return (InputStream)this.executeMethodInTX(getStream, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache, 
		    		key, forUpdate, time);
		}
		catch(RecoverException e){
			throw e;
		}
		catch(Throwable e){
			throw new RecoverException(e);
		}
    }

    /* métodos de remoção */
    
	public boolean remove(
			String key, Object value, long time) throws StorageException {
		
		try{
			return (Boolean)this.executeMethodInTX(removeExact, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
					key, value, time);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
    public boolean remove(
    		String key, long time) throws StorageException{       
		try{
			return (Boolean)this.executeMethodInTX(remove, 
					this.transactionManager.getCurrrent(), 
					this.transactionManager, this.cache,
		    		key, time);
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
