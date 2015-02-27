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
    
    volatile int countConnections;
    
    private Cache cache;
    
    private ExecutorService executorService;
    
    private boolean run;
    
    public BrCacheServer(int port, int maxConnections){
        this.run = false;
        this.maxConnections = maxConnections;
        this.port = port;
    }
    
    public void start() throws IOException{
        this.countConnections = 0;
        this.cache = new Cache();
        this.serverSocket = new ServerSocket(port);
        this.executorService = Executors.newFixedThreadPool(this.maxConnections);
        this.run = true;
        while(this.run){
            try{
                Terminal terminal = new Terminal(this.serverSocket, cache);
                terminal.init();
                this.executorService.submit(terminal);
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
    
    public static void main(String[] a) throws IOException{
        BrCacheServer server = new BrCacheServer(1044, 1);
        server.start();
    }
}
