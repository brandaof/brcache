/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.server;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 *
 * @author Brandao
 */
class TextBufferReader {
    
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
        this.result   = null;
    }

    public String readLine() throws IOException{
        byte[] data = readLineInBytes();
        return data == null? null : new String(data);
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
                
                if(len == -1)
                    throw new EOFException("premature end of data");
                
                this.limit += len;
            }
            
            if(this.offset == this.buffer.length){
                this.updateResult(this.buffer, start, this.offset - start - 1);
                this.hasLineFeed = false;
                this.offset = 1;
                this.limit  = 1;
                this.buffer[0] = this.buffer[this.buffer.length - 1];
                return this.result;
            }
            else
            if(this.offset > 0 && this.buffer[this.offset-1] == '\r' && this.buffer[this.offset] == '\n'){
                this.updateResult(this.buffer, start, this.offset - start - 1);
                this.hasLineFeed = true;
                this.offset++;
                return this.result;
            }
            else{
                this.offset++;
            }
        }
    }
    
    public byte[] readLineInBytes(int totalRead) throws IOException{
    	
        this.result = new byte[totalRead];
        this.offsetResult = 0;
        
        int remainingToMaxRead = totalRead;
        int read;
        
        while(remainingToMaxRead > 0){

            if(this.offset == this.limit){
                
                if(this.limit == this.capacity){
                    this.offset = 0;
                    this.limit  = 0;
                }
                
                int len = stream.read(this.buffer, this.limit, this.buffer.length - limit);
                
                if(len == -1)
                    throw new EOFException("premature end of data");
                
                this.limit += len;
            }
            
            int maxRead = this.limit - this.offset;
            if(remainingToMaxRead > maxRead)
            	read = maxRead;
            else
            	read = remainingToMaxRead;
            
            System.arraycopy(this.buffer, this.offset, this.result, this.offsetResult, read);
            this.offsetResult += read;
        	//this.updateResult(this.buffer, this.offset, read);
            this.offset += read;
            remainingToMaxRead -= read;
        }
        
        return this.result;
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
