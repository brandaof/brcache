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
import java.lang.reflect.Proxy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Representa um pool de conexões.
 * 
 * @author Brandao
 */
public class BrCacheConnectionPool {
    
    private int createdInstances;
    
    private final int minInstances;
    
    private final int maxInstances;

    private final String host;
    
    private final int port;
            
    private final BlockingQueue<BrCacheConnection> instances;

    /**
     * Cria um novo pool de conexões.
     * 
     * @param host Endereço do servidor.
     * @param port Porta que o servidor está escutando.
     * @param minInstances Conexões que serão iniciadas na criação da instância.
     * @param maxInstances Quantidade máxima de conexões que serão criadas.
     * @throws IOException Lançada se ocorrer alguma falha ao tentar iniciar as conexões.
     */
    public BrCacheConnectionPool(String host, int port, int minInstances, int maxInstances) 
            throws IOException{

        if(minInstances < 0)
            throw new IllegalArgumentException("minInstances");
        
        if(maxInstances < 1)
            throw new IllegalArgumentException("maxInstances");
        
        if(minInstances > maxInstances)
            throw new IllegalArgumentException("minInstances");

        this.host             = host;
        this.port             = port;
        this.minInstances     = minInstances;
        this.maxInstances     = maxInstances;
        this.instances        = new ArrayBlockingQueue<BrCacheConnection>(this.maxInstances);
        this.createdInstances = 0;
        
        for(int i=0;i<this.minInstances;i++){
            BrCacheConnection con = createConnection(host, port);
            this.instances.add(con);
        }
        
    }
    
    private BrCacheConnection createConnection(String host, int port) throws IOException{
        BrCacheConnectionImp con = new BrCacheConnectionImp(host, port);
        con.connect();
        return 
            (BrCacheConnection)Proxy.newProxyInstance(
                getClass().getClassLoader(), 
                new Class[]{BrCacheConnection.class}, 
                new BrConnectionInvocationHandler(this, con));
    }
    
    /**
     * Obtém uma conexão.
     * 
     * @return Conexão.
     * @throws InterruptedException Lançada se ocorrer uma falha ao tentar 
     * recuperar uma conexão.
     * @throws IOException Lançada se ocorrer uma falha ao tentar 
     * criar uma conexão.
     */
    public synchronized BrCacheConnection getConnection() 
            throws InterruptedException, IOException {
        
        BrCacheConnection con = this.instances.poll();
        
        if(con != null)
            return con;
        else
        if(this.createdInstances < this.maxInstances){
            con = createConnection(host, port);
            this.createdInstances++;
            return con;
        }
        else
            return this.instances.take();
        
    }

    /**
     * Tenta obtém uma conexão.
     * 
     * @return Conexão.
     * @throws InterruptedException Lançada se ocorrer uma falha ao tentar 
     * recuperar uma conexão.
     * @throws IOException Lançada se ocorrer uma falha ao tentar 
     * criar uma conexão.
     */
    public synchronized BrCacheConnection tryGetConnection(long l, TimeUnit tu) 
            throws InterruptedException, IOException {
        
        BrCacheConnection con = this.instances.poll();
        
        if(con != null)
            return con;
        else
        if(this.createdInstances < this.maxInstances){
            con = createConnection(host, port);
            con.connect();
            this.createdInstances++;
            return con;
        }
        else
            return this.instances.poll(l, tu);
        
    }
    
    /**
     * Libera o uso da conexão.
     * @param con Conexão.
     */
    synchronized void release(BrCacheConnection con){
        try{
            this.instances.put(con);
        }
        catch(Exception e){
            this.createdInstances--;
        }
    }

    /**
     * Remove a conexão do pool e libera o espaço para ser criada uma nova.
     * @param con Conexão.
     */
    synchronized void shutdown(BrCacheConnection con){
        this.createdInstances--;
        this.instances.remove(con);
    }
    
}
