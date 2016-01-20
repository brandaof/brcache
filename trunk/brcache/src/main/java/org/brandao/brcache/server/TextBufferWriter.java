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
import java.io.OutputStream;

/**
 *
 * @author Brandao
 */
class TextBufferWriter {
    
    private int offset;
    
    //private ByteBuffer buffer;
    private byte[] buffer;
    
    private int capacity;
    
    private OutputStream out;

    private boolean hasLineFeed;
    
    public TextBufferWriter(int capacity, OutputStream out){
        
        if(capacity < 1)
            throw new IllegalArgumentException("capacity");
        
        this.offset   = 0;
        this.buffer   = new byte[capacity];
        this.capacity = capacity;
        this.out      = out;
    }

    public void write(byte[] buffer) throws IOException{
        this.write(buffer, 0, buffer.length);
    }

    public void write(int i) throws IOException{
        this.write(new byte[]{(byte)(i & 0xff)}, 0, 1);
    }
    
    public void write(byte[] buffer, int offset, int len) throws IOException{
        int limitOffset  = offset + len;
        
        if(this.offset == this.capacity)
        	this.flush();
        
        while(offset < limitOffset){
            int maxRead  = limitOffset - offset;
            int maxWrite = this.capacity - this.offset;
            
            if(maxRead > maxWrite){
                System.arraycopy(buffer, offset, this.buffer, this.offset, maxWrite);
                offset      += maxWrite;
                this.offset += maxWrite;
                this.flush();
            }
            else{
                System.arraycopy(buffer, offset, this.buffer, this.offset, maxRead);
                offset       += maxRead;
                this.offset  += maxRead;
            }
        }
    }

    public OutputStream getDirectOutputStream(){
    	return this.out;
    }
    
    public void flush() throws IOException{
    	if(this.offset > 0){
	        this.out.write(this.buffer, 0, this.offset);
	        this.out.flush();
	        this.offset = 0;
    	}
    }

    public void close() throws IOException{
    	this.flush();
    }
    
    public void clear(){
        this.offset = 0;
    }

    public boolean isHasLineFeed() {
        return hasLineFeed;
    }

}
