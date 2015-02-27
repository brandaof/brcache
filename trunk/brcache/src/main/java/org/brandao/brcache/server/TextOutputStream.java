/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author Cliente
 */
public class TextOutputStream extends OutputStream{

    private OutputStream out;
    
    private boolean closed;
    
    public TextOutputStream(OutputStream out){
        this.out = out;
        this.closed = false;
    }
    
    @Override
    public void write(int i) throws IOException {
        
        if(closed)
            throw new IOException("stream closed");
        
        out.write(i);
    }
 
    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        
        if(closed)
            throw new IOException("stream closed");
        
        out.write(bytes, i, i1);
    }
    
    @Override
    public void flush() throws IOException{
        
        if(closed)
            throw new IOException("stream closed");
        
        out.flush();
    }
    
    @Override
    public void close() throws IOException{
        
        if(closed)
            throw new IOException("stream closed");
        
        this.closed = true;
    }
}
