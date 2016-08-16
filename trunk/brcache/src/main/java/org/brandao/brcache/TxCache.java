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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.brandao.brcache.tx.CacheTransaction;
import org.brandao.brcache.tx.CacheTransactionHandler;
import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.EntryCache;
import org.brandao.brcache.tx.TransactionException;

/**
 * Representa um cache.
 * 
 * @author Brandao
 */
public class TxCache 
	implements Serializable{
    
	private static final long serialVersionUID = -5345989492194115454L;

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
			
		}
		catch(Throwable e){
			throw new ExceptionInInitializerError(e);
		}
	}
	
	private StreamCache cache;
	
	private CacheTransactionManager transactionManager;

	private long transactionTimeout;
	
    public TxCache(CacheTransactionManager transactionManager){
        this(
    		3L*1024L*1024L, 1024, 0.5, 
    		1L*1024L*1024L, 1024, 0.5, 
    		10L*1024L*1024L, 64*1024, 1*1024, 0.5, 
    		1*1024*1024L, 100, "/mnt/brcache", SwaperStrategy.FILE, 1, transactionManager);
    }
    
    /**
     * Cria um novo cache.
     * 
     * @param nodeBufferSize Tamanho do buffer, em bytes, onde os nós ficarão armazenados. 
     * @param nodePageSize Tamanho da página, em bytes, do buffer de nós.
     * @param nodeSwapFactor Fator de permuta dos nós.
     * @param indexBufferSize Tamanho do buffer, em bytes, onde os índices ficarão armazenados.
     * @param indexPageSize Tamanho da página, em bytes, do buffer de índices.
     * @param indexSwapFactor Fator de permuta dos índices.
     * @param dataBufferSize Tamanho do buffer, em bytes, onde os dados ficarão armazenados. 
     * @param dataPageSize Tamanho da página, em bytes, do buffer de dados.
     * @param blockSize Tamanho do bloco, em bytes.
     * @param dataSwapFactor Fator de permuta dos dados.
     * @param maxSizeEntry Tamanho máximo de uma entrada no cache.
     * @param maxSizeKey Tamanho máximo de uma chave.
     * @param dataPath Pasta onde os dados do cache serão persistidos.
     * @param swaperType Estratégia de armazenamento dos dados em disco.
     * @param quantitySwaperThread Quantidade de processos usados para fazer a permuta.
     */
    public TxCache(
    		long nodeBufferSize,
    		long nodePageSize,
    		double nodeSwapFactor,
    		
    		long indexBufferSize,
    		long indexPageSize,
    		double indexSwapFactor,
    		
    		long dataBufferSize,
    		long dataPageSize,
    		long blockSize,
    		double dataSwapFactor,
    		
    		long maxSizeEntry,
    		int maxSizeKey,
            String dataPath,
            SwaperStrategy swaperType,
            int quantitySwaperThread,
            CacheTransactionManager transactionManager
    		){

    	this.cache = new StreamCache(nodeBufferSize, nodePageSize, nodeSwapFactor, 
    			indexBufferSize, indexPageSize, indexSwapFactor, dataBufferSize, dataPageSize, 
    			blockSize, dataSwapFactor, maxSizeEntry, maxSizeKey, dataPath, swaperType, 
    			quantitySwaperThread);
    	this.transactionManager = transactionManager;
    	this.transactionTimeout = TIME_OUT;
    }
    
    public long getTransactionTimeout() {
		return transactionTimeout;
	}

	public void setTransactionTimeout(long transactionTimeout) {
		this.transactionTimeout = transactionTimeout;
	}

	public CacheTransaction beginTransaction(){
    	return this.transactionManager.begin();
    }

	/* métodos de armazenamento */
	
	public Object replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		
		this.executeMethodInTX(method, currentTx, params);
	}
	
	public boolean replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object oldValue, 
			Object newValue, long maxAliveTime, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			if(o != null && o.equals(oldValue)){
				this.put(manager, cache, key, newValue, maxAliveTime, time);
				return true;
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	public Object putIfAbsent(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			
			if(o == null){
				this.put(manager, cache, key, value, maxAliveTime, time);
			}
			
			return o;
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	public void put(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		try{
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
			this.putStream(
				manager, cache, key, maxAliveTime, 
				new ByteArrayInputStream(bout.toByteArray()), time);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
    public void putStream(CacheTransactionManager manager, StreamCache cache, 
    		String key, long maxAliveTime, InputStream inputData, long time) 
    		throws StorageException {

    	try{
    		manager.tryLock(this.id, key, time);
			byte[] dta = 
				inputData == null? 
					null : 
					this.getBytes(inputData);
			this.managed.add(key);
			this.inserted.add(key);
			this.entities.put(key, new EntryCache(dta, maxAliveTime));
    	}
    	catch(Throwable e){
    		throw new StorageException(e);
    	}
    }
	
	/* métodos de coleta*/
	
	public Object get(CacheTransactionManager manager, StreamCache cache,
			String key, boolean forUpdate, long time) throws RecoverException {
		try{
			InputStream in = this.getStream(manager, cache, key, forUpdate, time);
			if(in != null){
					ObjectInputStream oin = new ObjectInputStream(in);
					return oin.readObject();
			}
			else
				return null;
		}
		catch(RecoverException e){
			throw e;
		}	
		catch(Throwable e){
			throw new RecoverException(e);
		}	
	}
    
    public InputStream getStream(CacheTransactionManager manager, StreamCache cache, 
    		String key, boolean forUpdate, long time) throws RecoverException {
    	
    	try{
			byte[] dta = this.getEntity(manager, cache, key, forUpdate, time);
			return dta == null? null : new ByteArrayInputStream(dta);
    	}
    	catch(RecoverException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new RecoverException(e);
    	}
    }

    /* métodos de remoção */
    
	public boolean remove(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			if(o != null && o.equals(value)){
				return this.remove(manager, cache, key, time);
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
    public boolean remove(CacheTransactionManager manager, StreamCache cache,
    		String key, long time) throws RecoverException{       
    	try{
    		manager.tryLock(this.id, key, time);
			this.managed.add(key);
			this.inserted.add(key);
			this.entities.put(key, null);
			return cache.getStream(key) != null;
    	}
    	catch(RecoverException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new RecoverException(e);
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

    private Object executeMethodInTX(Method method, 
    		CacheTransactionHandler currentTx, Object ... params) throws Throwable{
    	
    	CacheTransactionHandler tx = 
    			currentTx == null? 
    					this.transactionManager.begin() : 
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
