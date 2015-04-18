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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.server.ReadDataException;
import org.brandao.brcache.server.WriteDataException;

/**
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
    
    public BrCacheClient(String host, int port, 
            int minConnections, int maxConnections) throws CacheException {
        
        try{
            this.host = host;
            this.port = port;
            this.minConnections = minConnections;
            this.maxConnections = maxConnections;
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
    
    public void connect() throws IOException {
        this.pool =
            new BrCacheConnectionPool(host, port, minConnections, maxConnections);
    }

    public void disconect() throws IOException {
        this.close();
    }

    public void put(String key, long time, Object value) throws StorageException {
        try{
            tryExecuteAction(putMethod, new Object[]{key, time, value});
        }
        catch(Throwable e){
            throw new StorageException(e);
        }
    }

    public Object get(String key) throws RecoverException {
        try{
            return tryExecuteAction(getMethod, new Object[]{key});
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
    }

    public boolean remove(String key) throws RecoverException {
        try{
            return (Boolean)tryExecuteAction(removeMethod, new Object[]{key});
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public void close() throws IOException {
        if(this.pool != null)
            this.pool.shutdown();
    }
    
    private Object tryExecuteAction(Method method, Object[] args) throws Throwable{
        int countTry = 0;
        
        while(countTry < 360){
            countTry++;
            
            BrCacheConnection connection = null;
            try{
                connection = this.pool.getConnection();
                Object result = method.invoke(connection, args);
                this.pool.release(connection);
                return result;
            }
            catch(Throwable e){
                e.printStackTrace();
                Throwable i = e;
                while(i != null){
                    
                    if(i instanceof InvocationTargetException)
                        i = ((InvocationTargetException)i).getTargetException();
                    else
                    if(i instanceof UndeclaredThrowableException) 
                        i = ((UndeclaredThrowableException)i).getUndeclaredThrowable();
                    else                        
                    if(i instanceof ReadDataException || i instanceof WriteDataException || i instanceof IOException){
                        try{
                            if(connection != null)
                                this.pool.shutdown(connection);
                        }
                        catch(Throwable ex){}
                        break;
                    }
                    else
                        i = i.getCause();
                }
                
                if(i == null){
                    if(connection != null)
                        this.pool.release(connection);
                    throw e;
                }
                
                try{
                    Thread.sleep(1000);
                }
                catch(InterruptedException ex){
                }
                
            }
            
        }
        
        throw new CacheException("communication fail");
    }    
}
