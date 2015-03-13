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

package org.brandao.brcache.client;

import java.io.Closeable;
import java.io.IOException;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

/**
 * Permite o armazenamento, atualização, remoção de um item em um servidor BRCache.
 * 
 * @author Brandao.
 */
public interface BrCacheConnection extends Closeable{
    
    /**
     * Faz a conexão com o servidor.
     * 
     * @throws IOException Lançada caso ocorra alguma falha ao tentar se
     * conectar ao servidor.
     */
    void connect() throws IOException;
    
    /**
     * Fecha a conexão com o servidor.
     * 
     * @throws IOException Lançada caso ocorra alguma falha ao tentar se
     * fechar a conexão com o servidor.
     */
    void disconect() throws IOException;
    
    /**
     * Inclui ou sobrescreve um objeto no cache.
     * 
     * @param key Identificação do objeto no cache.
     * @param time Tempo máximo em milesegundos que o objeto ficará no cache.
     * @param value Objeto a ser incluído no cache.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * objeto no cache.
     */
    void put(String key, long time, Object value) throws StorageException;
    
    /**
     * Recupera um objeto do cache.
     * 
     * @param key Identificação do objeto no cache.
     * @return Objeto ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * objeto do cache.
     */
    Object get(String key) throws RecoverException;

    /**
     * Remove um objeto do cache.
     * 
     * @param key Identificação do item no cache.
     * @return Verdadeiro se o item for removido. Caso contrário falso.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
     */
    boolean remove(String key) throws RecoverException;
    
    /**
     * Obtém o endereço do servidor.
     * @return Endereço do servidor.
     */
    String getHost();

    /**
     * Obtém a porta do servidor.
     * @return Porta do servidor.
     */
    int getPort();
    

}
