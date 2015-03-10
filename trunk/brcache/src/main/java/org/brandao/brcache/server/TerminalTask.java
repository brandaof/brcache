/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.net.Socket;
import org.brandao.brcache.Cache;

/**
 *
 * @author Cliente
 */
public class TerminalTask implements Runnable{

    private final Terminal terminal;
    
    private final TerminalFactory factory;
    
    private final Configuration config;
    
    private Cache cache;
    
    private Socket socket;
    
    private int readBufferSize;
    
    private int writeBufferSize;
    
    public TerminalTask(Terminal terminal, Cache cache, Socket socket, 
            int readBufferSize, int writeBufferSize, 
            TerminalFactory factory,
            Configuration config){
        this.terminal        = terminal;
        this.factory         = factory;
        this.cache           = cache;
        this.socket          = socket;
        this.readBufferSize  = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.config          = config;
    }
    
    public void run() {
        try{
            updateInfo();
            this.terminal.init(this.socket, this.cache, this.readBufferSize, this.writeBufferSize);
            this.terminal.execute();
        }
        catch(Exception e){
            //e.printStackTrace();
        }
        finally{
            try{
                terminal.destroy();
            }
            catch(Exception e){
            }
            this.factory.release(this.terminal);
            updateInfo();
        }
    }
    
    private void updateInfo(){
        synchronized(TerminalTask.class){
            this.config.setProperty("curr_connections", String.valueOf(this.factory.getCurrentInstances()));
            this.config.setProperty("total_connections", String.valueOf(this.factory.getCountInstances()));
        }
    }
}
