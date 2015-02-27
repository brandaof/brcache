/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.BufferedReader;
import java.io.IOException;

/**
 *
 * @author Cliente
 */
public class StringBuffer {
    
    private int offset;
    
    private StringBuilder buffer;
    
    private int capacity;
    
    private BufferedReader reader;

    private boolean hasLineFeed;
    
    public StringBuffer(int capacity, BufferedReader reader){
        this.offset = 0;
        this.buffer = new StringBuilder(offset);
        this.capacity = capacity;
        this.reader = reader;
    }
    
    public StringBuilder readLine() throws IOException{
        int count = 0;
        int start = this.offset;
        while(true){

            if(this.offset == buffer.length()){
                char[] tmp = new char[2048];
                int len = reader.read(tmp);
                this.buffer.append(tmp, 0, len);
            }
            
            if(this.offset > 0 && this.buffer.charAt(this.offset-1) == '\r' && this.buffer.charAt(this.offset) == '\n'){
                StringBuilder result = 
                        this.offset == 0?
                            new StringBuilder() :
                            (new StringBuilder()).append(this.buffer, start, this.offset - 1);
                this.buffer.delete(start, this.offset + 1);
                this.offset = 0;
                this.hasLineFeed = true;
                return result;
            }
            else
            if(count > this.capacity){
                StringBuilder result = 
                        this.offset == 0?
                            new StringBuilder() :
                            (new StringBuilder()).append(this.buffer, start, this.offset);
                this.buffer.delete(start, this.offset);
                this.offset = 0;
                this.hasLineFeed = false;
                return result;
            }

            this.offset++;
            count ++;
        }
    }
    
    public void clear(){
        this.buffer.setLength(0);
        this.offset = 0;
    }

    public boolean isHasLineFeed() {
        return hasLineFeed;
    }

}
