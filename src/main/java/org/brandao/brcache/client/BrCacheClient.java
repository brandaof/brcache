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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;

import org.brandao.brcache.CacheException;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.server.DefaultStreamFactory;
import org.brandao.brcache.server.StreamFactory;

/**
 * Cliente para um servidor BrCache.
 * 
 * @author Brandao
 */
public class BrCacheClient implements BrCacheConnection{

    private String host;
    
    private int port;
    
    private int minConnections;
    
    private int maxConnections;
    
    private BrCacheConnectionPool pool;
    
    private final Method putMethod;

    private final Method getMethod;
    
    private final Method removeMethod;

    private StreamFactory streamFactory;
    
    /**
     * Cria um novo cliente.
     * 
     * @param host Endereço do servidor.
     * @param port Porta de conexão com o servidor.
     * @param minConnections Número de conexões que serão inicialmente criadas.
     * @param maxConnections Número máximo de conexões.
     * @throws CacheException Lançada caso ocorra algum problema ao fazer as 
     * configurações iniciais.
     */
    
    public BrCacheClient(String host, int port, 
            int minConnections, int maxConnections) throws CacheException {
        this(host, port, minConnections, maxConnections, new DefaultStreamFactory());
    }
    
    /**
     * Cria um novo cliente.
     * 
     * @param host Endereço do servidor.
     * @param port Porta de conexão com o servidor.
     * @param minConnections Número de conexões que serão inicialmente criadas.
     * @param maxConnections Número máximo de conexões.
     * @param streamFactory Fábrica que produz os objetos que auxiliarão na 
     * manipulação do fluxo de dados das conexões.
     * @throws CacheException Lançada caso ocorra algum problema ao fazer as 
     * configurações iniciais.
     */
    
    public BrCacheClient(String host, int port, 
            int minConnections, int maxConnections, StreamFactory streamFactory) throws CacheException {
        
        try{
            this.host = host;
            this.port = port;
            this.minConnections = minConnections;
            this.maxConnections = maxConnections;
            this.streamFactory = streamFactory;
            
            this.putMethod = 
                    BrCacheConnection.class
                            .getMethod("put", new Class[] {String.class, long.class, Object.class});
            this.getMethod = 
                    BrCacheConnection.class
                            .getMethod("get", new Class[] {String.class});
            this.removeMethod = 
                    BrCacheConnection.class
                            .getMethod("remove", new Class[] {String.class});
        }
        catch(Throwable e){
            throw new CacheException(e);
        }
    }
    
    /**
     * Inicia as conexões com o servidor.
     * 
     * @throws IOException Lançada caso não seja possível a conexão com o servidor.
     */
    public void connect() throws IOException {
        this.pool =
            new BrCacheConnectionPool(host, port, minConnections, maxConnections, streamFactory);
    }

    /**
     * Destrói as conexões com o servidor.
     * 
     * @see BrCacheClient#close() 
     * @throws IOException Lançada caso ocorra alguma falha ao 
     * tentar desconectar do servidor.
     */
    public void disconect() throws IOException {
        this.close();
    }

    /**
     * Insere um item no cache.
     * @param key Identificação do item no cache.
     * @param time Tempo máximo de vida do item no cache.
     * @param value Item.
     * @throws StorageException Lançada caso ocorra alguma falha ao tentar 
     * inserir o item no cache.
     */
    public void put(String key, long time, Object value) throws StorageException {
        try{
            tryExecuteAction(putMethod, new Object[]{key, time, value});
        }
        catch(Throwable e){
            throw new StorageException(e);
        }
    }

    /**
     * Recupera um item do cache.
     * @param key Identificação do item no cache.
     * @return Item.
     * @throws RecoverException Lançada caso ocorra alguma falha ao tentar 
     * recuperar o item do cache.
     */
    public Object get(String key) throws RecoverException {
        try{
            return tryExecuteAction(getMethod, new Object[]{key});
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
    }

    /**
     * Remove um item do cache.
     * @param key Identificação do item no cache.
     * @return Verdadeiro se o item for removido. Caso contrário falso.
     * @throws RecoverException Lançada se ocorrer alguma falha ao 
     * tentar remover o item do cache.
     */
    public boolean remove(String key) throws RecoverException {
        try{
            return (Boolean)tryExecuteAction(removeMethod, new Object[]{key});
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
    }

    /**
     * Obtém o endereço do servidor.
     * @return Endereço.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Obtém a porta o servidor.
     * @return Porta.
     */
    public int getPort() {
        return this.port;
    }

    /**
     * @see Closeable#close() 
     * @throws IOException 
     */
    public void close() throws IOException {
        if(this.pool != null)
            this.pool.shutdown();
    }
    
    private Object tryExecuteAction(Method method, Object[] args) throws Throwable{
        int countTry = 0;
        Throwable x = null;
        
        while(countTry < 360){
            countTry++;
            
            BrCacheConnection connection = null;
            boolean release = false;
            try{
                connection = this.pool.getConnection();
                Object result = method.invoke(connection, args);
                release = true;
                //this.pool.release(connection);
                return result;
            }
            catch(Throwable e){
                
            	Throwable i = e;
                
            	while(i instanceof InvocationTargetException || i instanceof UndeclaredThrowableException){
            		if(i instanceof InvocationTargetException)
            			i = ((InvocationTargetException)i).getTargetException();
            		else
            		if(i instanceof UndeclaredThrowableException)
            			i = ((UndeclaredThrowableException)i).getUndeclaredThrowable();
                }
                
                x = i;
                
            	if(e instanceof ConnectException || i.getCause() instanceof IOException){
            		release = false;
            	}
                else{
                	release = true;
                    throw i;
                }
            	
                try{
                    Thread.sleep(1000);
                }
                catch(InterruptedException ex){
                }
                
            }
            finally{
            	try{
	            	if(connection != null){
		            	if(release){
		            		this.pool.release(connection);
		            	}
		            	else{
		                    this.pool.shutdown(connection);
		            	}
	            	}
            	}
            	catch(Throwable ex){
            	}
            }
            
        }
        
        throw x;
    }    
    
}
