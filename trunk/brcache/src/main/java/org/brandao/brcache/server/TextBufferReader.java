/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 *
 * @author Cliente
 */
public class TextBufferReader {
    
    private int offset;
    
    private int limit;
    
    private byte[] buffer;
    
    private int capacity;
    
    private InputStream stream;

    private boolean hasLineFeed;
    
    private byte[] result;
    
    private int offsetResult;
    
    public TextBufferReader(int capacity, InputStream stream){
        this.offset   = 0;
        this.limit    = 0;
        this.buffer   = new byte[capacity];
        this.capacity = capacity;
        this.stream   = stream;
    }

    public StringBuilder readLine() throws IOException{
        byte[] data = readLineInBytes();
        return data == null? null : new StringBuilder(new String(data));
    }
    
    public byte[] readLineInBytes() throws IOException{
        this.result = new byte[0];
        this.offsetResult = 0;
        
        int start = this.offset;
        while(true){

            if(this.offset == this.limit){
                
                if(this.limit == this.capacity){
                    
                    if(start < this.limit){
                        this.updateResult(this.buffer, start, this.offset - start - 1);
                        this.buffer[0] = this.buffer[this.buffer.length - 1];
                        this.offset = 1;
                        this.limit  = 1;
                    }
                    else{
                        this.offset = 0;
                        this.limit  = 0;
                    }
                    
                    start  = 0;
                }
                
                int len = stream.read(this.buffer, this.limit, this.buffer.length - limit);
                if(this.limit == -1)
                    throw new IOException();
                
                this.limit += len;
            }
            
            if(this.offset > 0 && this.buffer[this.offset-1] == '\r' && this.buffer[this.offset] == '\n'){
                this.updateResult(this.buffer, start, this.offset - start - 1);
                this.hasLineFeed = true;
                this.offset++;
                return this.result;
            }
            else
            if(this.offset == this.buffer.length){
                this.updateResult(this.buffer, start, this.offset - start - 1);
                this.hasLineFeed = false;
                this.offset = 1;
                this.limit  = 1;
                this.buffer[0] = this.buffer[this.buffer.length - 1];
                return this.result;
            }
            else{
                this.offset++;
            }
        }
    }
    
    private void updateResult(byte[] data, int offset, int len){
        
        if(len == 0)
            return;
        
        if(this.result == null)
            this.result = new byte[len];
        else
            this.result = Arrays.copyOf(this.result, this.result.length + len);
        
        System.arraycopy(data, offset, this.result, this.offsetResult, len);
        this.offsetResult += len;
        
    }
    
    public void clear(){
        this.offset = 0;
        this.limit  = 0;
    }

    public boolean isHasLineFeed() {
        return hasLineFeed;
    }
    
}
