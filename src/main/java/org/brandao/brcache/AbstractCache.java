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
    	this.cacheHandler = new CacheHandler(name, config);
    }

    
    /**
	 * Associa o fluxo de bytes do valor à chave.
	 * @param key chave associada ao fluxo.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o item for substituido. Caso contrário, <code>false</code>
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return cacheHandler.putStream(key, inputData, timeToLive, timeToIdle);
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
    	return cacheHandler.replaceStream(key, inputData, timeToLive, timeToIdle);
    }
    
    /**
     * Associa o fluxo de bytes do valor à chave somente se a chave não estiver associada a um valor.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return fluxo associado à chave ou <code>null</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item ou se o item atual 
     * expirar no momento da execução do método.
     */
    public InputStream putIfAbsentStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return cacheHandler.putIfAbsentStream(key, inputData, timeToLive, timeToIdle);
    }
    
    /**
     * Obtém o fluxo de bytes do valor associado à chave.
     * @param key chave associada ao fluxo.
     * @return fluxo de bytes do valor ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
     */
    
    public InputStream getStream(String key) throws RecoverException {
    	return cacheHandler.getStream(key);
    }
    
    /**
     * Verifica se uma chave está associada a um valor.
     * @param key chave associada ao valor.
     * @return <code>true</code> se a chave estiver associada a um valor. Caso contrário, <code>false</code>.
     */
    public boolean containsKey(String key){
    	return cacheHandler.containsKey(key);
    }
    
    /**
     * Obtém a quantidade de itens recuperados.
     * @return Quantidade de itens recuperados.
     */
    public long getCountRead(){
        return cacheHandler.getCountRead();
    }

    /**
     * Obtém a quantidade de itens armazenados.
     * @return Quantidade de itens armazenados.
     */
    public long getCountWrite(){
        return cacheHandler.getCountWrite();
    }

    /**
     * Obtém a quantidade de itens removidos.
     * @return Quantidade de itens removidos.
     */
    public long getCountRemoved() {
		return cacheHandler.getCountRemoved();
	}

    /**
     * Obtém a quantidade de bytes recuperados.
     * @return Quantidade de bytes recuperados.
     */
    public long getCountReadData() {
        return cacheHandler.getCountReadData();
    }
    
    /**
     * Obtém a quantidade de bytes armazenados.
     * @return Quantidade de bytes armazenados.
     */
    public long getCountWriteData() {
        return cacheHandler.getCountWriteData();
    }

    /**
     * Obtém a quantidade de bytes removidos.
     * @return Quantidade de bytes removidos.
     */
    public long getCountRemovedData() {
        return cacheHandler.getCountRemovedData();
    }
    
    /**
     * Verifica se os arquivos contidos na pasta de dados serão 
     * destruidos junto com essa instância. 
     * @return <code>true</code> para destruir todos os arquivos. 
     * Caso contrário, <code>false</code>.
     */
    public boolean isDeleteOnExit() {
		return cacheHandler.isDeleteOnExit();
	}

    /**
     * Define que os arquivos contidos na pasta de dados sejam 
     * destruidos junto com essa instância. 
     * @param deleteOnExit <code>true</code> para destruir todos os arquivos. 
     * Caso contrário, <code>false</code>.
     */
	public void setDeleteOnExit(boolean deleteOnExit) {
		cacheHandler.setDeleteOnExit(deleteOnExit);
	}

	/**
	 * Remove todas as entradas contidas no cache.
	 */
	public void clear(){
		cacheHandler.clear();
	}
	
	/**
	 * Destrói os dados contidos nesta instância. Deve ser executado se {@link #isDeleteOnExit()} 
	 * for <code>false</code>.
	 */
	public void destroy(){
		cacheHandler.destroy();
	}

}
