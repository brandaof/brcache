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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.brandao.brcache.BasicCache;
import org.brandao.brcache.Cache;
import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
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
 *        cache.put("chave1", objeto1);
 *        cache.remove("chave2");
 *        cache.putIfAbsent("chave3", objeto3);
 *        tx.commit();
 *    }
 *    catch(Throwable e){
 *        tx.rollback();
 *    }
 * </pre>
 * 
 * @author Brandao
 */
public class TXCache 
	extends Cache
	implements Serializable{
    
	private static final long serialVersionUID = 1L;

	private static final long TIME_OUT = 5*60*1000;
	
	private static final Method replace;

	private static final Method replaceStream;
	
	private static final Method replaceExact;

	private static final Method putIfAbsent;

	private static final Method putIfAbsentStream;
	
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
					BasicCache.class, String.class, Object.class, long.class, long.class, long.class);

			replaceStream = CacheTransactionHandler.class.getDeclaredMethod(
					"replaceStream", CacheTransactionManager.class, 
					BasicCache.class, String.class, InputStream.class, long.class, long.class, long.class);
			
			replaceExact = CacheTransactionHandler.class.getDeclaredMethod(
					"replace", CacheTransactionManager.class, BasicCache.class,
					String.class, Object.class, 
					Object.class, long.class, long.class, long.class);

			putIfAbsent = CacheTransactionHandler.class.getDeclaredMethod(
					"putIfAbsent", CacheTransactionManager.class, BasicCache.class,
					String.class, Object.class, long.class, long.class, long.class);

			putIfAbsentStream = CacheTransactionHandler.class.getDeclaredMethod(
					"putIfAbsentStream", CacheTransactionManager.class, BasicCache.class,
					String.class, InputStream.class, long.class, long.class, long.class);
			
			put = CacheTransactionHandler.class.getDeclaredMethod(
					"put", CacheTransactionManager.class, BasicCache.class,
					String.class, Object.class, long.class, long.class, long.class);

			putStream = CacheTransactionHandler.class.getDeclaredMethod(
					"putStream", CacheTransactionManager.class, BasicCache.class, 
		    		String.class, InputStream.class, long.class, long.class, long.class);

			get = CacheTransactionHandler.class.getDeclaredMethod(
					"get", CacheTransactionManager.class, BasicCache.class,
					String.class, boolean.class, long.class);

			getStream = CacheTransactionHandler.class.getDeclaredMethod(
					"getStream", CacheTransactionManager.class, BasicCache.class, 
		    		String.class, boolean.class, long.class);

			removeExact = CacheTransactionHandler.class.getDeclaredMethod(
					"remove", CacheTransactionManager.class, BasicCache.class,
					String.class, Object.class, long.class);

			remove = CacheTransactionHandler.class.getDeclaredMethod(
					"remove", CacheTransactionManager.class, BasicCache.class,
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
	
	private BasicCache cache;
	
	private CacheTransactionManager transactionManager;

	private long transactionTimeout;

	/**
	 * Cria um cache transacional a partir de um cache.
	 * @param cache cache não transacional.
	 */
    public TXCache(BasicCache cache){
    	this(cache, new CacheTransactionManagerImp() , TIME_OUT);
    }
	
    /**
     * Cria um cache transacional especificando o gestor transacional.
     * @param cache cache não transacional.
     * @param transactionManager gestor transacional.
     */
    public TXCache(BasicCache cache, CacheTransactionManager transactionManager){
    	this(cache, transactionManager, TIME_OUT);
    }

    /**
     * Cria um cache transacional especificando o gestor transacional e o tempo limite.
     * @param cache cache não transacional.
     * @param transactionManager gestor transacional.
     * @param timeout tempo limite. É o tempo máximo que se espera, em milisegundos, para concluir uma operação
     * no cache.
     */
    public TXCache(BasicCache cache, CacheTransactionManager transactionManager, long timeout){
    	this.cache = cache;
    	this.transactionManager = transactionManager;
    	this.transactionTimeout = timeout;
    	
    	this.transactionManager.setConfiguration(cache.getConfig());
    }
    
    /**
     * Obtém o gestor transacional.
     * @return gestor transacional.
     */
    public CacheTransactionManager getTransactionManager(){
    	return this.transactionManager;
    }
    
    /**
     * Obtém o tempo limite de uma operação.
     * @return tempo em milisegundos.
     */
    public long getTransactionTimeout() {
		return transactionTimeout;
	}

    /**
     * Define o tempo limite de uma operação.
     * @param transactionTimeout tempo em milisegundos.
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
     * @param key chave associada ao valor.
     * @param value valor para ser associado à chave.
     * @param maxAliveTime tempo máximo de vida do valor no cache.
     * @return o valor anterior associado à chave.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
	public boolean replace(
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		try{
			return (Boolean)this.executeMethodInTX(replace, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
					key, value, timeToLive, timeToIdle, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
    /**
     * Substitui o fluxo de bytes associado à chave somente se ele existir.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public boolean replaceStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
		try{
			return (Boolean)this.executeMethodInTX(replaceStream, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
					key, inputData, timeToLive, timeToIdle, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
    }
    
	/**
	 * Substitui o valor associado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associada ao valor.
	 * @param oldValue valor esperado associado à chave.
	 * @param newValue valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
	 * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
	 */
	public boolean replace(
			String key, Object oldValue, Object newValue, long timeToLive, long timeToIdle) throws StorageException {
		
		try{
			return (Boolean)this.executeMethodInTX(replaceExact, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
					key, oldValue, 
					newValue, timeToLive, timeToIdle, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}

	}
	
	/**
	 * Associa o valor a chave somente se a chave não estiver associada a um valor.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
	 * @return valor anterior associado à chave.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
	 */
	public Object putIfAbsent(
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		try{
			return this.executeMethodInTX(putIfAbsent, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
					key, value, timeToLive, timeToIdle, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
    /**
     * Associa o fluxo de bytes do valor à chave somente se a chave não estiver associada a um valor.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return fluxo associado à chave ou <code>null</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public InputStream putIfAbsentStream(String key, 
    		InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
		try{
			return (InputStream)this.executeMethodInTX(putIfAbsentStream, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
					key, inputData, timeToLive, timeToIdle, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
	/**
	 * Associa o valor à chave.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
	 */
	public boolean put(
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		try{
			return (Boolean)this.executeMethodInTX(put, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
					key, value, timeToLive, timeToIdle, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
    /**
	 * Associa o fluxo de bytes do valor à chave.
	 * @param key chave associada ao fluxo.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
     * @param inputData fluxo de bytes do valor.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public boolean putStream(
    		String key, InputStream inputData, long timeToLive, long timeToIdle) 
    		throws StorageException {
		try{
			return (Boolean)this.executeMethodInTX(putStream, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache, 
		    		key, inputData, timeToLive, timeToIdle, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
    }
	
	/* métodos de coleta*/
	
	/**
	 * Obtém o valor associado à chave.
	 * @param key chave associada ao valor.
     * @return valor associado à chave ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
	 */
	public Object get(
			String key) throws RecoverException {
		return this.get(key, false);
	}
    
	/**
     * Obtém o valor associado à chave bloqueando ou não 
     * seu acesso as demais transações.
     * @param key chave associada ao valor.
     * @param forUpdate <code>true</code> para bloquear o item. Caso contrário <code>false</code>.
     * @return valor associado à chave ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
	 */
	public Object get(
			String key, boolean forUpdate) throws RecoverException {
		try{
			return this.executeMethodInTX(get, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
					key, forUpdate, this.transactionTimeout);
		}
		catch(RecoverException e){
			throw e;
		}
		catch(CacheException e){
			throw new RecoverException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new RecoverException(e, CacheErrors.ERROR_1021);
		}
	}
    
    /**
     * Obtém o fluxo de bytes do valor associado à chave.
     * @param key chave associada ao fluxo.
     * @return fluxo de bytes do valor ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
     */
    public InputStream getStream( 
    		String key) throws RecoverException {
    	return this.getStream(key, false);
    }
    
    /**
     * Obtém o fluxo de bytes do valor associado à chave bloqueando ou não 
     * seu acesso as demais transações.
     * @param key chave associada ao fluxo.
     * @param forUpdate <code>true</code> para bloquear o item. Caso contrário <code>false</code>.
     * @return fluxo de bytes do valor ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
     */
    public InputStream getStream( 
    		String key, boolean forUpdate) throws RecoverException {
    	
		try{
			return (InputStream)this.executeMethodInTX(getStream, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache, 
		    		key, forUpdate, this.transactionTimeout);
		}
		catch(RecoverException e){
			throw e;
		}
		catch(CacheException e){
			throw new RecoverException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new RecoverException(e, CacheErrors.ERROR_1021);
		}
    }

    /* métodos de remoção */
    
	/**
	 * Remove o valor assoiado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associada ao valor.
	 * @return valor para ser associado à chave.
	 * @return <code>true</code> se o valor for removido. Caso contrário, <code>false</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
	 */
	public boolean remove(
			String key, Object value) throws StorageException {
		
		try{
			return (Boolean)this.executeMethodInTX(removeExact, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
					key, value, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
	/**
	 * Remove o valor associado à chave.
	 * @param key chave associada ao valor.
	 * @return <code>true</code> se o valor for removido. Caso contrário, <code>false</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
	 */
    public boolean remove(
    		String key) throws StorageException{       
		try{
			return (Boolean)this.executeMethodInTX(remove, 
					this.transactionManager.getCurrrent(false), 
					this.transactionManager, this.cache,
		    		key, this.transactionTimeout);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
    }	
    
    /**
     * Obtém a quantidade de itens recuperados.
     * @return Quantidade de itens recuperados.
     */
    public long getCountRead(){
        return this.cache.getCountRead();
    }

    /**
     * Obtém a quantidade de itens armazenados.
     * @return Quantidade de itens armazenados.
     */
    public long getCountWrite(){
        return this.cache.getCountWrite();
    }

    /**
     * Obtém a quantidade de itens removidos.
     * @return Quantidade de itens removidos.
     */
    public long getCountRemoved() {
		return this.cache.getCountRemoved();
	}

    /**
     * Obtém a quantidade de bytes recuperados.
     * @return Quantidade de bytes recuperados.
     */
    public long getCountReadData() {
        return this.cache.getCountReadData();
    }
    
    /**
     * Obtém a quantidade de bytes armazenados.
     * @return Quantidade de bytes armazenados.
     */
    public long getCountWriteData() {
        return this.cache.getCountWriteData();
    }

    /**
     * Obtém a quantidade de bytes removidos.
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
    		Object r = method.invoke(tx, params);
			if(currentTx == null){
				tx.commit();
			}
			return r;
    	}
    	catch(IllegalAccessException e){
    		throw new CacheException(new Exception("bug!", e), CacheErrors.ERROR_1023);
    	}
    	catch(IllegalArgumentException e){
    		throw new CacheException(new Exception("bug!", e), CacheErrors.ERROR_1023);
    	}
    	catch(InvocationTargetException e){
    		Throwable ex = e.getTargetException();
    		
    		try{
    			if(currentTx == null){
    				tx.rollback();
    			}
    		}
			catch(Throwable x){
	    		throw new CacheException(
    				new Exception(
						"bug: exception not recognized (rollback fail): " + x.toString(), ex), 
						CacheErrors.ERROR_1018);
				
			}
    		
    		if(ex instanceof StorageException || ex instanceof RecoverException){
        		throw ex;
    		}
    		else{
        		throw new CacheException(new Exception("bug: exception not recognized: ", e), CacheErrors.ERROR_1023);
    		}
    		
    	}
    	/*
    	finally{
    		try{
    			this.transactionManager.close(tx);
    		}
    		catch(TransactionException e){
    			throw new StorageException(e);    			
    		}
    	}
    	*/
    	
    }
    
}
