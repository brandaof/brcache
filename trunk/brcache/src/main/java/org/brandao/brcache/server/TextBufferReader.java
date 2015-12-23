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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

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
    
    private ByteBuffer result;
    
    private int offsetResult;
    
    public TextBufferReader(int capacity, InputStream stream){
        this.offset   = 0;
        this.limit    = 0;
        this.buffer   = new byte[capacity];
        this.capacity = capacity;
        this.stream   = stream;
        this.result   = ByteBuffer.allocateDirect(capacity);
    }

    public String readLine() throws IOException{
        byte[] data = readLineInBytes();
        return data == null? null : new String(data);
    }

    public byte[] readLineInBytes() throws IOException{
    	return this.readLineInBytes(-1);
    }
    
    public byte[] readLineInBytes(int maxRead) throws IOException{
    	
    	int remainingToMaxRead = maxRead;
    	
        this.result.clear();//this.result = new byte[0];
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
                    throw new IOException("premature end of data");
                
                this.limit += len;
            }
            
            if(this.offset == this.buffer.length){
                this.updateResult(this.buffer, start, this.offset - start - 1);
                this.hasLineFeed = false;
                this.offset = 1;
                this.limit  = 1;
                this.buffer[0] = this.buffer[this.buffer.length - 1];
                this.result.flip();
                byte[] r = new byte[this.result.limit()];
                this.result.get(r);
                return r;
            }
            else
            if(maxRead != -1 && remainingToMaxRead == 1){
                this.updateResult(this.buffer, start, this.offset - start + 1);
                this.hasLineFeed = false;
                this.offset++;
                this.result.flip();
                byte[] r = new byte[this.result.limit()];
                this.result.get(r);
                return r;
            }
            else
            if(maxRead == -1 && this.offset > 0 && this.buffer[this.offset-1] == '\r' && this.buffer[this.offset] == '\n'){
                this.updateResult(this.buffer, start, this.offset - start - 1);
                this.hasLineFeed = true;
                this.offset++;
                this.result.flip();
                byte[] r = new byte[this.result.limit()];
                this.result.get(r);
                return r;
            }
            else{
                this.offset++;
                remainingToMaxRead--;
            }
        }
    }
    
    private void updateResult(byte[] data, int offset, int len){
        if(len == 0)
            return;
        
        if(this.result.position() + len > this.result.capacity()){
        	ByteBuffer newByteBuffer = ByteBuffer.allocateDirect(this.result.position() + len);
        	this.result.flip();
        	newByteBuffer.put(this.result);
        	this.result = newByteBuffer;
        }
        
        this.result.put(data, offset, len);
    }

    /*
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
    */
    
    public void clear(){
        this.offset = 0;
        this.limit  = 0;
    }

    public boolean isHasLineFeed() {
        return hasLineFeed;
    }
    
}
