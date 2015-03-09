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
    
    private Cache cache;
    
    private Socket socket;
    
    private int readBufferSize;
    
    private int writeBufferSize;
    
    public TerminalTask(Terminal terminal, Cache cache, Socket socket, 
            int readBufferSize, int writeBufferSize, TerminalFactory factory){
        this.terminal        = terminal;
        this.factory         = factory;
        this.cache           = cache;
        this.socket          = socket;
        this.readBufferSize  = readBufferSize;
        this.writeBufferSize = writeBufferSize;
    }
    
    public void run() {
        try{
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
        }
    }
    
}
