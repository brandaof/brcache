/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

/**
 *
 * @author Cliente
 */
public class StringBufferReader {
    
    private int offset;
    
    private int limit;
    
    private byte[] buffer;
    
    private int capacity;
    
    private InputStream stream;

    private boolean hasLineFeed;
    
    private Charset charset;
    
    private CharsetDecoder decoder;
    
    public StringBufferReader(int capacity, InputStream stream){
        this.offset   = 0;
        this.limit    = 0;
        this.buffer   = new byte[capacity];
        this.capacity = capacity;
        this.stream   = stream;
        this.charset  = Charset.defaultCharset();
        this.decoder  = this.charset.newDecoder();
    }

    public StringBuilder readLine() throws IOException{
        byte[] data = readLineInBytes();
        return new StringBuilder(new String(data));
    }
    
    public byte[] readLineInBytes() throws IOException{
        int start = this.offset;
        while(true){

            if(this.offset == this.limit){
                if(this.limit == this.capacity){
                    this.offset = 0;
                    this.limit  = 0;
                }
                int len = stream.read(this.buffer, this.limit, this.buffer.length - limit);
                if(this.limit == -1)
                    throw new IOException();
                
                this.limit += len;
            }
            
            if(this.offset > 0 && this.buffer[this.offset-1] == '\r' && this.buffer[this.offset] == '\n'){
                byte[] result = new byte[this.offset - start - 1];
                System.arraycopy(this.buffer, start, result, 0, result.length);
                this.hasLineFeed = true;
                this.offset++;
                return result;
            }
            else
            if(this.offset == this.limit){
                byte[] result = new byte[this.offset - start];
                System.arraycopy(this.buffer, start, result, 0, result.length);
                this.hasLineFeed = false;
                this.offset++;
                return result;
            }

            this.offset++;
        }
    }
    
    public void clear(){
        this.offset = 0;
        this.limit  = 0;
    }

    public boolean isHasLineFeed() {
        return hasLineFeed;
    }

    private char[] bytesToStringUTFCustom(byte[] bytes, int start, int offset) {
        char[] buffer = new char[offset - start];
        int index = 0;
        for(int i = start; i < offset; i++) {
            char c = (char)bytes[i];
            buffer[index++] = c;
        }
        return buffer;
    }
    
}
