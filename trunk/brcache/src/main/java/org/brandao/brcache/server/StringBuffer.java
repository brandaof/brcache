/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

/**
 *
 * @author Cliente
 */
public class StringBuffer {
    
    private int offset;
    
    private StringBuilder buffer;
    
    private int capacity;
    
    public StringBuffer(int capacity){
        this.offset = 0;
        this.buffer = new StringBuilder(offset);
        this.capacity = capacity;
    }
    
    public void append(char[] data){
        buffer.append(data, 0, data.length);
    }
    
    public void append(char[] data, int start, int offset){
        buffer.append(data, start, offset);
    }
    
    public StringBuilder readLine(){
        int count = 0;
        for(int i=this.offset;i<buffer.length();i++){
            
            if(this.buffer.charAt(i) == '\n' || count > this.capacity){
                StringBuilder result = (new StringBuilder()).append(this.buffer,this.offset, i);
                this.offset = i + 1;
                return result;
            }
        }
        
        if(this.offset == buffer.length())
            return null;
        else{
            StringBuilder result = (new StringBuilder()).append(this.buffer);
            this.clear();
            return result;
        }
    }
    
    public void clear(){
        this.buffer.setLength(0);
        this.offset = 0;
    }
}
