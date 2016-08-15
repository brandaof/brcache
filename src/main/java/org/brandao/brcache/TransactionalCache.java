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

import org.brandao.brcache.tx.CacheTransaction;
import org.brandao.brcache.tx.CacheTransactionHandler;
import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.TransactionException;

/**
 * Representa um cache.
 * 
 * @author Brandao
 */
public class TransactionalCache 
	implements Serializable{
    
	private static final long serialVersionUID = -5345989492194115454L;

	private static final long TIME_OUT = 5*60*1000;
	
	private NonTransactionalCache cache;
	
	private CacheTransactionManager transactionManager;

	private long transactionTimeout;
	
    public TransactionalCache(CacheTransactionManager transactionManager){
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
    public TransactionalCache(
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

    	this.cache = new NonTransactionalCache(nodeBufferSize, nodePageSize, nodeSwapFactor, 
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
    
    /**
     * Inclui ou sobrescreve um objeto no cache.
     * 
     * @param key Identificação do objeto no cache.
     * @param maxAliveTime Tempo máximo em milesegundos que o objeto ficará no cache.
     * @param item Objeto a ser incluído no cache.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * objeto no cache.
     */
    public void putObject(String key, long maxAliveTime, Object item) throws StorageException{
    	
    	CacheTransactionHandler tx = this.transactionManager.getCurrrent();
    	
    	if(tx == null){
    		this.putObjectLocalTX(key, maxAliveTime, item);
    	}
    	else{
    		tx.putObject(this.transactionManager, cache, key, maxAliveTime, item);
    	}
    	
    }

    private void putObjectLocalTX(String key, long maxAliveTime, Object item) throws StorageException{
    	
    	CacheTransactionHandler tx = this.transactionManager.begin();
    	
    	try{
    		tx.putObject(this.transactionManager, this.cache, key, maxAliveTime, item);
			tx.commit();
    	}
    	catch(StorageException e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new StorageException("rollback fail: " + x.toString(), e); 
			}
    		
    		throw e;
    	}
    	catch(Throwable e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new StorageException(
						"bug: exception not recognized (rollback fail): " + x.toString(), e); 
			}
    		
    		throw new StorageException("bug: exception not recognized", e);
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
    
    /**
     * Recupera um objeto do cache.
     * 
     * @param key Identificação do objeto no cache.
     * @return Objeto ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * objeto do cache.
     */
    public Object getObject(String key) throws RecoverException{
    	
    	CacheTransactionHandler tx = this.transactionManager.getCurrrent();
    	
    	if(tx == null){
    		return this.getObjectLocalTX(key, false);
    	}
    	else{
    		return tx.getObject(this.transactionManager, cache, key, false, this.transactionTimeout);
    	}
    	
    }

    public Object getObject(String key, boolean forUpdate) throws RecoverException{
    	
    	CacheTransactionHandler tx = this.transactionManager.getCurrrent();
    	
    	if(tx == null){
    		throw new RecoverException("transaction not stated");
    	}
    	else{
    		return tx.getObject(this.transactionManager, cache, key, false, this.transactionTimeout);
    	}
    	
    }
    
    private Object getObjectLocalTX(String key, boolean forUpdate) throws RecoverException{
    	
    	CacheTransactionHandler tx = this.transactionManager.begin();
    	
    	try{
    		Object o = tx.getObject(this.transactionManager, cache, key, forUpdate, this.transactionTimeout);
			tx.commit();
			return o;
    	}
    	catch(RecoverException e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new RecoverException("rollback fail: " + x.toString(), e); 
			}
    		
    		throw e;
    	}
    	catch(Throwable e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new RecoverException(
						"bug: exception not recognized (rollback fail): " + x.toString(), e); 
			}
    		
    		throw new RecoverException("bug: exception not recognized", e);
    	}
    	finally{
    		try{
    			this.transactionManager.close(tx);
    		}
    		catch(TransactionException e){
    			throw new RecoverException(e);    			
    		}
    	}
    	
    }
    
    /**
     * Inclui ou sobrescreve um item no cache.
     * 
     * @param key Identificação do item no cache.
     * @param maxAliveTime Tempo máximo em milesegundos que o item ficará no cache.
     * @param inputData Fluxo de dados que representa o item.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
     */
    public void put(String key, long maxAliveTime, InputStream inputData) throws StorageException{
        
    	CacheTransactionHandler tx = this.transactionManager.getCurrrent();
    	
    	if(tx == null){
    		this.putLocalTX(key, maxAliveTime, inputData);
    	}
    	else{
    		tx.put(this.transactionManager, cache, key, maxAliveTime, inputData, this.transactionTimeout);
    	}
    	
    }

    private void putLocalTX(String key, long maxAliveTime, InputStream inputData) throws StorageException{
    	CacheTransactionHandler tx = this.transactionManager.begin();
    	
    	try{
    		tx.put(this.transactionManager, cache, key, maxAliveTime, inputData, this.transactionTimeout);
			tx.commit();
    	}
    	catch(StorageException e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new StorageException("rollback fail: " + x.toString(), e); 
			}
    		
    		throw e;
    	}
    	catch(Throwable e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new StorageException(
						"bug: exception not recognized (rollback fail): " + x.toString(), e); 
			}
    		
    		throw new StorageException("bug: exception not recognized", e);
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
    
    /**
     * Recupera um item do cache.
     * 
     * @param key Identificação do item no cache.
     * @return Fluxo de dados que representa o item ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * item do cache.
     */
    public InputStream get(String key) throws RecoverException{
    	
    	CacheTransactionHandler tx = this.transactionManager.getCurrrent();
    	
    	if(tx == null){
    		return this.getLocalTX(key, false);
    	}
    	else{
    		return tx.get(this.transactionManager, cache, key, false, this.transactionTimeout);
    	}
    	
    }

    public InputStream get(String key, boolean forUpdate) throws RecoverException{
    	
    	CacheTransactionHandler tx = this.transactionManager.getCurrrent();
    	
    	if(tx == null){
    		throw new RecoverException("transaction not stated");
    	}
    	else{
    		return tx.get(this.transactionManager, cache, key, false, this.transactionTimeout);
    	}
    	
    }
    
    private InputStream getLocalTX(String key, boolean forUpdate) throws RecoverException{
    	
    	CacheTransactionHandler tx = this.transactionManager.begin();
    	
    	try{
    		InputStream o = tx.get(this.transactionManager, cache, key, forUpdate, this.transactionTimeout);
			tx.commit();
			return o;
    	}
    	catch(RecoverException e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new RecoverException("rollback fail: " + x.toString(), e); 
			}
    		
    		throw e;
    	}
    	catch(Throwable e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new RecoverException(
						"bug: exception not recognized (rollback fail): " + x.toString(), e); 
			}
    		
    		throw new RecoverException("bug: exception not recognized", e);
    	}
    	finally{
    		try{
    			this.transactionManager.close(tx);
    		}
    		catch(TransactionException e){
    			throw new RecoverException(e);    			
    		}
    	}
    	
    }
    
    /**
     * Remove um item do cache.
     * 
     * @param key Identificação do item no cache.
     * @return Verdadeiro se o item for removido. Caso contrário falso.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
     */
    public boolean remove(String key) throws RecoverException{
    	CacheTransactionHandler tx = this.transactionManager.getCurrrent();
    	
    	if(tx == null){
    		return this.removeLocalTX(key);
    	}
    	else{
    		return tx.remove(this.transactionManager, cache, key, this.transactionTimeout);
    	}
    }
    
    private boolean removeLocalTX(String key) throws RecoverException{
    	
    	CacheTransactionHandler tx = this.transactionManager.begin();
    	
    	try{
    		boolean o = tx.remove(this.transactionManager, cache, key, this.transactionTimeout);
			tx.commit();
			return o;
    	}
    	catch(RecoverException e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new RecoverException("rollback fail: " + x.toString(), e); 
			}
    		
    		throw e;
    	}
    	catch(Throwable e){
    		try{
    			tx.rollback();
    		}
			catch(Throwable x){
				throw new RecoverException(
						"bug: exception not recognized (rollback fail): " + x.toString(), e); 
			}
    		
    		throw new RecoverException("bug: exception not recognized", e);
    	}
    	finally{
    		try{
    			this.transactionManager.close(tx);
    		}
    		catch(TransactionException e){
    			throw new RecoverException(e);    			
    		}
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
    
}
