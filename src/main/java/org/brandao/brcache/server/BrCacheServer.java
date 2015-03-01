/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.brandao.brcache.Cache;

/**
 *
 * @author Cliente
 */
public class BrCacheServer {
    
    private ServerSocket serverSocket;
    
    private int port;
    
    private int maxConnections;
    
    private int minConnections;
    
    private int timeout;
    
    private boolean reuseAddress;
    
    volatile int countConnections;
    
    private Cache cache;
    
    private TerminalFactory terminalFactory;
    
    private ExecutorService executorService;
    
    private boolean run;
    
    public BrCacheServer(
            int port, 
            int minConnections, 
            int maxConnections, 
            int timeout, 
            boolean reuseAddress,
            Cache cache){
        this.run            = false;
        this.timeout        = timeout;
        this.reuseAddress   = reuseAddress;
        this.maxConnections = maxConnections;
        this.minConnections = minConnections;
        this.port           = port;
        this.cache          = cache;
    }
    
    public void start() throws IOException{
        this.terminalFactory = new TerminalFactory(this.minConnections, this.maxConnections);
        this.serverSocket = new ServerSocket(this.port, 1);
        this.serverSocket.setSoTimeout(this.timeout);
        this.serverSocket.setReuseAddress(this.reuseAddress);
        this.executorService = Executors.newFixedThreadPool(this.maxConnections);
        
        this.run = true;
        while(this.run){
            try{
                Terminal terminal = this.terminalFactory.getInstance();
                TerminalTask task = 
                    new TerminalTask(
                            terminal, 
                            this.cache, 
                            this.serverSocket.accept(), 
                            this.terminalFactory);
                
                this.executorService.execute(task);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    
    public void stop() throws IOException{
        this.run = false;
        try{
            executorService.shutdownNow();
        }
        finally{
            this.serverSocket.close();
        }
    }
    
}
