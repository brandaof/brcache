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
import java.util.Arrays;

/**
 *
 * @author Brandao
 */
public abstract class AbstractTextInputStreamReader extends InputStream{

    private static final byte[] CRLF = TerminalConstants.CRLF;
	
    protected TextBufferReader buffer;
    
    protected byte[] byteBuffer;
    
    private int offsetBuf;
    
    private boolean closed;
    
    private boolean hasLineFeed;
    
    public AbstractTextInputStreamReader(TextBufferReader buffer, int offset){
        this.buffer      = buffer;
        this.byteBuffer  = null;
        this.offsetBuf   = offset;
        this.closed      = false;
        this.hasLineFeed = false;
    }
    
    @Override
    public int read() throws IOException {
        byte[] buf = new byte[1];
        int result = this.read(buf, 0, 1);
        
        if(result == -1)
            return -1;
        else
            return buf[0] & 0xff;
        
    }
    
    @Override
    public int read(byte[] bytes, int offset, int len) throws IOException {    
        
        if(closed)
            return -1;
        
        int initOffset = offset;
        int limitRead  = offset + len;
        boolean insertlinefeed;
        
        while(!closed && offset < limitRead){
            
            int maxRead  = byteBuffer == null? 0 : byteBuffer.length - this.offsetBuf;
            int maxWrite = limitRead - offset;

            if(maxRead == 0){
                byte[] line = this.readData(this.buffer);
                if(line == null){
                	this.close();
                    return offset - initOffset;
                }
                
                insertlinefeed = this.hasLineFeed;
                this.hasLineFeed = this.buffer.isHasLineFeed();
                
                if(insertlinefeed){
                    this.byteBuffer = Arrays.copyOf(CRLF, CRLF.length + line.length);
                    System.arraycopy(line, 0, this.byteBuffer, CRLF.length, line.length);
                }
                else
                    this.byteBuffer = line;
                            
                this.offsetBuf = 0;
                maxRead = byteBuffer.length - this.offsetBuf;
            }
            
            if(maxWrite > maxRead){
                System.arraycopy(
                        this.byteBuffer, 
                        this.offsetBuf, 
                        bytes, 
                        offset, 
                        maxRead);
                
                this.offsetBuf += maxRead;
                offset         += maxRead;
            }
            else{
                System.arraycopy(
                    this.byteBuffer, 
                    this.offsetBuf, 
                    bytes, 
                    offset, 
                    maxWrite);
                
                this.offsetBuf += maxWrite;
                offset         += maxWrite;
            }            
        }
        
        return offset - initOffset;
    }
    
    protected abstract byte[] readData(TextBufferReader buffer) throws IOException;
    
    @Override
    public void close() throws IOException{
        if(this.closed)
            return;
            
        this.closed = this.closeData(this.buffer);
   }
    
    protected abstract boolean closeData(TextBufferReader buffer) throws IOException;
}
