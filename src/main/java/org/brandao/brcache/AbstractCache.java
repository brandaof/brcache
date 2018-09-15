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

/**
 * É a base para um cache. Ele faz o mapeamento chave-fluxo de 
 * bytes. Uma chave somente pode estar associado a um 
 * fluxo de bytes. Não são permtidos chaves duplicadas. 
 * 
 * <pre>
 * ex:
 *    
 *    Cache cache = ...;
 *    InputStream valueStream = ...;
 *    cache.putStream("uma_chave", valueStream, 1200, 0);
 *    
 * ex2:
 *    
 *    InputStream value = cache.getStream("uma_chave");
 *
 * </pre>
 * 
 * @author Brandao
 */
public abstract class AbstractCache implements Cache, Serializable{
    
    private static final long serialVersionUID = 8023029671447700902L;

    protected CacheHandler cacheHandler;
    
    /**
     * Cria um novo cache.
     * 
     * @param name Nome do cache.
     * @param config Configuração do cache.
     */
    public AbstractCache(String name, BRCacheConfig config){
    	this.cacheHandler = new BasicCacheHandler(name, config);
    }

    /**
     * Cria um novo cache.
     * 
     * @param cacheHandler Manpulador do cache.
     */
    public AbstractCache(CacheHandler cacheHandler){
    	
    	if(cacheHandler == null){
    		throw new NullPointerException("cacheHandler");
    	}
    	
    	this.cacheHandler = cacheHandler;
    }
    
    
    public boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return cacheHandler.putStream(key, inputData, timeToLive, timeToIdle);
    }

    public boolean replaceStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return cacheHandler.replaceStream(key, inputData, timeToLive, timeToIdle);
    }
    
    public InputStream putIfAbsentStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return cacheHandler.putIfAbsentStream(key, inputData, timeToLive, timeToIdle);
    }
    
    public InputStream getStream(String key) throws RecoverException {
    	return cacheHandler.getStream(key);
    }
    
    public boolean remove(String key) throws StorageException{
    	return cacheHandler.removeStream(key);
    }
    
    public boolean containsKey(String key){
    	return cacheHandler.containsKey(key);
    }
    
    public long getCountRead(){
        return cacheHandler.getCountRead();
    }

    public long getCountWrite(){
        return cacheHandler.getCountWrite();
    }

    public long getCountRemoved() {
		return cacheHandler.getCountRemoved();
	}

    public long getCountReadData() {
        return cacheHandler.getCountReadData();
    }
    
    public long getCountWriteData() {
        return cacheHandler.getCountWriteData();
    }

    public long getCountRemovedData() {
        return cacheHandler.getCountRemovedData();
    }
    
    public boolean isDeleteOnExit() {
		return cacheHandler.isDeleteOnExit();
	}

	public void setDeleteOnExit(boolean deleteOnExit) {
		cacheHandler.setDeleteOnExit(deleteOnExit);
	}

	public long size() {
		return cacheHandler.size();
	}
	
	public boolean isEmpty() {
		return cacheHandler.isEmpty();
	}
	
	public void clear(){
		cacheHandler.clear();
	}
	
	public void destroy(){
		cacheHandler.destroy();
	}

}
