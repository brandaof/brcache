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
    
    private final Configuration config;
    
    private final BlockingQueue<Terminal> instances;

    private int countInstances;
    
    private int currentInstances;
    
    public TerminalFactory(Configuration config, int minInstances, int maxInstances){

        if(minInstances < 0)
            throw new IllegalArgumentException("minInstances");
        
        if(maxInstances < 1)
            throw new IllegalArgumentException("maxInstances");
        
        if(minInstances > maxInstances)
            throw new IllegalArgumentException("minInstances");

        this.config           = config;
        this.minInstances     = minInstances;
        this.maxInstances     = maxInstances;
        this.instances        = new ArrayBlockingQueue<Terminal>(this.maxInstances);
        this.createdInstances = 0;
        
        for(int i=0;i<this.minInstances;i++)
            this.instances.add(new Terminal(config));
        
    }
    
    public synchronized Terminal getInstance() throws InterruptedException {
        
        Terminal terminal = this.instances.poll();
        
        if(terminal == null){
            if(this.createdInstances < this.maxInstances){
                terminal = new Terminal(config);
                this.createdInstances++;
            }
            else
                terminal = this.instances.take();
        }
        
        this.countInstances++;
        this.currentInstances++;
        return terminal;
    }

    public synchronized Terminal tryGetInstance(long l, TimeUnit tu) throws InterruptedException {
        
        Terminal terminal = this.instances.poll();
        
        if(terminal == null){
            if(this.createdInstances < this.maxInstances){
                terminal = new Terminal(this.config);
                this.createdInstances++;
            }
            else
                terminal = this.instances.poll(l, tu);
        }
        
        this.countInstances++;
        this.currentInstances++;
        return terminal;
    }
    
    public synchronized void release(Terminal terminal){
        this.currentInstances--;
        this.instances.add(terminal);
    }
    
    public int getMinInstances() {
        return minInstances;
    }

    public int getMaxInstances() {
        return maxInstances;
    }

    public synchronized int getCountInstances() {
        return countInstances;
    }

    public synchronized int getCurrentInstances() {
        return currentInstances;
    }
    
}
