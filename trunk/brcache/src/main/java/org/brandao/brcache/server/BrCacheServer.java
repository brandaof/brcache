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

package org.brandao.brcache.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.brandao.brcache.Cache;
import org.brandao.brcache.Memory;
import org.brandao.brcache.SwaperStrategy;
import org.brandao.brcache.collections.Collections;

/**
 * Representa o servidor do cache.
 * 
 * @author Brandao
 */
public class BrCacheServer {
    
    private ServerSocket serverSocket;
    
    private int port;
    
    private int maxConnections;
    
    private int timeout;
    
    private boolean reuseAddress;
    
    volatile int countConnections;
    
    private Cache cache;
    
    private int readBufferSize;
    
    private int writeBufferSize;
    
    private TerminalFactory terminalFactory;
    
    private ExecutorService executorService;
    
    private Configuration config;
    
    private MonitorThread monitorThread;
    
    private boolean run;
    
    private boolean compress;
    
    private StreamFactory streamFactory;
    
    /**
     * Cria uma nova instância do cache.
     * 
     * @param config Configuração.
     */
    public BrCacheServer(Configuration config){
        this.loadConfiguration(config);
    }
    
    /**
     * Cria uma nova instância do cache.
     * 
     * @param port Porta que o servidor irá escutar.
     * @param maxConnections Número máximo de conexões permitidas.
     * @param timeout Define o timeout da conexão em milesegundos.
     * @param reuseAddress Liga ou desliga a opção do socket SO_REUSEADDR.
     * @param cache Cache.
     */
    public BrCacheServer(
            int port, 
            int maxConnections, 
            int timeout, 
            boolean reuseAddress,
            Cache cache){
        this.run            = false;
        this.timeout        = timeout;
        this.reuseAddress   = reuseAddress;
        this.maxConnections = maxConnections;
        this.port           = port;
        this.cache          = cache;
    }
    
    /**
     * Inicia o servidor.
     * 
     * @throws IOException Lançada se ocorrer alguma falha ao tentar iniciar 
     * o servidor.
     */
    public void start() throws IOException{
        this.terminalFactory = new TerminalFactory(this.config, 0, this.maxConnections);
        this.serverSocket = new ServerSocket(this.port);
        this.serverSocket.setSoTimeout(this.timeout);
        this.serverSocket.setReuseAddress(this.reuseAddress);
        this.executorService = Executors.newFixedThreadPool(this.maxConnections);
        this.streamFactory = this.createStreamFactory();
        
        this.run = true;
        while(this.run){
            Socket socket = null;
            try{
                socket = this.serverSocket.accept();
                Terminal terminal = this.terminalFactory.getInstance();
                TerminalTask task = 
                    new TerminalTask(
                            terminal, 
                            this.cache, 
                            socket, 
                            this.streamFactory,
                            this.readBufferSize,
                            this.writeBufferSize,
                            this.terminalFactory,
                            this.config);
                
                this.executorService.execute(task);
            }
            catch(Exception e){
                //e.printStackTrace();
            }
        }
    }
    
    /**
     * Para o servidor.
     * 
     * @throws IOException Lançada se ocorrer alguma falha ao tentar parar 
     * o servidor.
     */
    public void stop() throws IOException{
        this.run = false;
        try{
            executorService.shutdownNow();
        }
        finally{
            this.serverSocket.close();
        }
    }
    
    private StreamFactory createStreamFactory(){
        StreamFactory factory;
        if(this.compress)
            factory = new CompressStreamFactory();
        else
            factory = new DefaultStreamFactory();
        
        factory.setConfiguration(this.config);
        return factory;
    }
    
    private void loadConfiguration(Configuration config){

        long portNumber            = config.getLong("port","8084");
        long max_connections       = config.getLong("max_connections","1024");
        long timeout_connection    = config.getLong("timeout_connection","0");
        boolean reuse_address      = config.getBoolean("reuse_address", "false");
        long nodes_size            = config.getLong("nodes_size","10m");
        long nodes_swap_size       = config.getLong("nodes_swap_size","16k");
        double nodes_swap_factor   = config.getDouble("nodes_swap_factor","0.3");
        long index_size            = config.getLong("index_size","10m");
        long index_swap_size       = config.getLong("index_swap_size","16k");
        double index_swap_factor   = config.getDouble("index_swap_factor","0.3");
        long data_size             = config.getLong("data_size","100m");
        long data_swap_size        = config.getLong("data_swap_size","128k");
        double data_swap_factor    = config.getDouble("data_swap_factor","0.6");
        String data_path           = config.getString("data_path","/var/brcache");
        long max_slab_size         = config.getLong("max_slab_size","16k");
        long write_buffer_size     = config.getLong("write_buffer_size","16k");
        long read_buffer_size      = config.getLong("read_buffer_size","16k");
        long max_size_entry        = config.getLong("max_size_entry","1m");
        long max_size_key          = config.getLong("max_size_key","48");
        String swapper             = config.getString("swapper","file_tree");
        int swapper_thread         = config.getInt("swapper_thread","1");
        double lock_factor         = config.getDouble("lock_factor","0.1");
        boolean compressState      = config.getBoolean("compress_stream","false");
        
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
        
        this.run             = false;
        this.config          = config;
        this.timeout         = (int)timeout_connection;
        this.reuseAddress    = reuse_address;
        this.maxConnections  = (int)max_connections;
        this.port            = (int)portNumber;
        this.readBufferSize  = (int)read_buffer_size;
        this.writeBufferSize = (int)write_buffer_size;
        this.compress        = compressState;
        
        Collections.setPath(data_path);
        
        this.cache = new Cache(
            nodes_size,
            nodes_swap_size,
            nodes_swap_factor,
            index_size,
            index_swap_size,
            index_swap_factor,
            data_size,
            data_swap_size,
            data_swap_factor,
            (int)max_slab_size,
            (int)max_size_entry,
            (int)max_size_key,
            data_path,
            SwaperStrategy.valueOf(swapper.toUpperCase()),
            lock_factor,
            swapper_thread);
        
        Memory.allocOnly((long)(data_size / 2));
        this.monitorThread = new MonitorThread(this.cache, this.config);
        this.monitorThread.start();
    }

    public StreamFactory getStreamFactory() {
        return streamFactory;
    }
    
}
