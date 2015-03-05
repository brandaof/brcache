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

    private StringBufferWriter buffer;
    
    private boolean closed;
    
    public TextOutputStream(StringBufferWriter buffer){
        this.buffer = buffer;
        this.closed = false;
    }
    
    @Override
    public void write(int i) throws IOException {
        
        if(closed)
            throw new IOException("stream closed");
        
        this.buffer.write(i);
    }
 
    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        
        if(closed)
            throw new IOException("stream closed");
        
        this.buffer.write(bytes, i, i1);
    }
    
    @Override
    public void flush() throws IOException{
        
        if(closed)
            throw new IOException("stream closed");
        
        this.buffer.flush();
    }
    
    @Override
    public void close() throws IOException{
        
        if(closed)
            throw new IOException("stream closed");
        
        this.closed = true;
    }
}
