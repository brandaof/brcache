/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Cliente
 */
public class TerminalFactory {
    
    private int createdInstances;
    
    private final int minInstances;
    
    private final int maxInstances;
    
    private final BlockingQueue<Terminal> instances;

    public TerminalFactory(int minInstances, int maxInstances){

        if(minInstances < 0)
            throw new IllegalArgumentException("minInstances");
        
        if(maxInstances < 1)
            throw new IllegalArgumentException("maxInstances");
        
        if(minInstances > maxInstances)
            throw new IllegalArgumentException("minInstances");

        this.minInstances     = minInstances;
        this.maxInstances     = maxInstances;
        this.instances        = new ArrayBlockingQueue<Terminal>(this.maxInstances);
        this.createdInstances = 0;
        
        for(int i=0;i<this.minInstances;i++)
            this.instances.add(new Terminal());
        
    }
    
    public synchronized Terminal getInstance() throws InterruptedException {
        
        Terminal terminal = this.instances.poll();
        
        if(terminal != null)
            return terminal;
        else
        if(this.createdInstances < this.maxInstances){
            terminal = new Terminal();
            this.createdInstances++;
            return terminal;
        }
        else
            return this.instances.take();
        
    }

    public synchronized Terminal tryGetInstance(long l, TimeUnit tu) throws InterruptedException {
        
        Terminal terminal = this.instances.poll();
        
        if(terminal != null)
            return terminal;
        else
        if(this.createdInstances < this.maxInstances){
            terminal = new Terminal();
            this.createdInstances++;
            return terminal;
        }
        else
            return this.instances.poll(l, tu);
        
    }
    
    public void release(Terminal terminal){
        this.instances.add(terminal);
    }
    
    public int getMinInstances() {
        return minInstances;
    }

    public int getMaxInstances() {
        return maxInstances;
    }
    
}
