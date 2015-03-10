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
public class StringBufferWriter {
    
    private int offset;
    
    private byte[] buffer;
    
    private int capacity;
    
    private OutputStream out;

    private boolean hasLineFeed;
    
    public StringBufferWriter(int capacity, OutputStream out){
        
        if(capacity < 1)
            throw new IllegalArgumentException("capacity");
        
        this.offset = 0;
        this.buffer = new byte[capacity];
        this.capacity = capacity;
        this.out = out;
    }

    public void write(byte[] buffer) throws IOException{
        this.write(buffer, 0, buffer.length);
    }

    public void write(int i) throws IOException{
        this.write(new byte[]{(byte)i}, 0, 1);
    }
    
    public void write(byte[] buffer, int offset, int len) throws IOException{
        
        int limitOffset  = offset + len;
        
        while(offset < limitOffset){
            int maxRead  = limitOffset - offset;
            int maxWrite = this.capacity - this.offset;
            
            if(maxRead > maxWrite){
                System.arraycopy(buffer, offset, this.buffer, this.offset, maxWrite);
                offset += maxWrite;
                this.offset += maxWrite;
                this.flush();
            }
            else{
                System.arraycopy(buffer, offset, this.buffer, this.offset, maxRead);
                offset       += maxRead;
                this.offset += maxRead;
            }
        }
    }

    public void flush() throws IOException{
        this.out.write(this.buffer, 0, this.offset);
        this.out.flush();
        this.offset = 0;
    }
    
    public void clear(){
        this.offset = 0;
    }

    public boolean isHasLineFeed() {
        return hasLineFeed;
    }

}
