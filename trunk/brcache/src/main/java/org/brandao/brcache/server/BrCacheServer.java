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
    
    private int readBufferSize;
    
    private TerminalFactory terminalFactory;
    
    private ExecutorService executorService;
    
    private Configuration config;
    
    private MonitorThread monitorThread;
    
    private boolean run;
    
    public BrCacheServer(Configuration config){
        this.loadConfiguration(config);
    }
    
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
        this.terminalFactory = new TerminalFactory(this.config, this.minConnections, this.maxConnections);
        this.serverSocket = new ServerSocket(this.port);
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
                            this.readBufferSize,
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
    
    private void loadConfiguration(Configuration config){

        int port                  = config.getInt("port","1044");
        int max_connections       = config.getInt("max_connections","1024");
        int timeout_connection    = config.getInt("timeout_connection","0");
        boolean reuse_address     = config.getBoolean("reuse_address", "false");
        double nodes_size         = config.getDouble("nodes_size","10m");
        double nodes_swap_size    = config.getDouble("nodes_swap_size","16k");
        double nodes_swap_factor  = config.getDouble("nodes_swap_factor","0.3");
        double index_size         = config.getDouble("index_size","10m");
        double index_swap_size    = config.getDouble("index_swap_size","16k");
        double index_swap_factor  = config.getDouble("index_swap_factor","0.3");
        double data_size          = config.getDouble("data_size","100m");
        double data_swap_size     = config.getDouble("data_swap_size","128k");
        double data_swap_factor   = config.getDouble("data_swap_factor","0.6");
        String data_path          = config.getString("data_path","/var/brcache");
        int max_slab_size         = config.getInt("max_slab_size","16k");
        int write_buffer_size     = config.getInt("write_buffer_size","16k");
        int read_buffer_size     = config.getInt("read_buffer_size","16k");
        int max_size_entry        = config.getInt("max_size_entry","1m");
        int max_size_key          = config.getInt("max_size_key","48");
        
        
        if(nodes_swap_size > nodes_size)
            throw new RuntimeException("nodes_swap_size > nodes_size");

        if(index_swap_size > index_swap_size)
            throw new RuntimeException("index_swap_size > index_swap_size");

        if(max_slab_size > data_swap_size)
            throw new RuntimeException("max_slab_size > data_swap_size");

        if(data_swap_size/max_slab_size < 1.0)
            throw new RuntimeException("data_swap_size must be greater than " + max_slab_size);

        if(data_swap_size > data_size)
            throw new RuntimeException("data_swap_size > data_size");
        
        double nodesOnMemory          = nodes_size/8.0;
        double nodesPerSegment        = nodes_swap_size/8.0;
        double swapSegmentNodesFactor = nodes_swap_factor;
        
        double indexOnMemory          = index_size/40.0;
        double indexPerSegment        = index_swap_size/40.0;
        double swapSegmentIndexFactor = index_swap_factor;
        
        double bytesOnMemory          = data_size/max_slab_size;
        double bytesPerSegment        = data_swap_size/max_slab_size;
        double swapSegmentsFactor     = data_swap_factor;
        
        String path                   = data_path;
        int maxBytesStoragePerGroup   = max_slab_size;
        int writeBufferSize           = write_buffer_size;
        int maxBytesToStorageEntry    = max_size_entry;
        int maxLengthKey              = max_size_key;
        
        this.run            = false;
        this.config         = config;
        this.timeout        = timeout_connection;
        this.reuseAddress   = reuse_address;
        this.maxConnections = max_connections;
        this.minConnections = 0;
        this.port           = port;
        this.readBufferSize = read_buffer_size;
        
        
        this.cache = new Cache(
            nodesOnMemory,
            nodesPerSegment,
            swapSegmentNodesFactor,
            indexOnMemory,
            indexPerSegment,
            swapSegmentIndexFactor,
            bytesOnMemory,
            bytesPerSegment,
            swapSegmentsFactor,
            path,
            maxBytesStoragePerGroup,
            writeBufferSize,
            maxBytesToStorageEntry,
            maxLengthKey);
        
        this.monitorThread = new MonitorThread(this.cache, this.config);
        this.monitorThread.start();
        
    }
}
