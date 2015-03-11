/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.client;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Cliente
 */
public class BrCacheConnectionPool {
    
    private int createdInstances;
    
    private final int minInstances;
    
    private final int maxInstances;

    private final String host;
    
    private final int port;
            
    private final BlockingQueue<BrCacheConnection> instances;

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
            BrCacheConnection con = new BrCacheConnection(host, port);
            con.connect();
            this.instances.add(con);
        }
        
    }
    
    public synchronized BrCacheConnection getConnection() throws InterruptedException, IOException {
        
        BrCacheConnection con = this.instances.poll();
        
        if(con != null)
            return con;
        else
        if(this.createdInstances < this.maxInstances){
            con = new BrCacheConnection(host,port);
            con.connect();
            this.createdInstances++;
            return con;
        }
        else
            return this.instances.take();
        
    }

    public synchronized BrCacheConnection tryGetConnection(long l, TimeUnit tu) throws InterruptedException, IOException {
        
        BrCacheConnection con = this.instances.poll();
        
        if(con != null)
            return con;
        else
        if(this.createdInstances < this.maxInstances){
            con = new BrCacheConnection(host,port);
            con.connect();
            this.createdInstances++;
            return con;
        }
        else
            return this.instances.poll(l, tu);
        
    }
    
    public void release(BrCacheConnection con){
        try{
            this.instances.put(con);
        }
        catch(Exception e){
            
        }
    }
    
    public int getMinConnections() {
        return minInstances;
    }

    public int getMaxConnections() {
        return maxInstances;
    }
    
}
