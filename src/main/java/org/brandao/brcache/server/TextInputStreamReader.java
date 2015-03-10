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
public class TextInputStreamReader extends InputStream{

    private static final byte[] CRLF = "\r\n".getBytes();
    
    private static final byte[] END = "END".getBytes();
    
    private StringBufferReader buffer;
    
    private byte[] byteBuffer;
    
    private int offsetBuf;
    
    private boolean closed;
    
    private boolean hasLineFeed;
    
    public TextInputStreamReader(StringBufferReader buffer, int offset){
        this.buffer = buffer;
        this.byteBuffer = null;
        this.offsetBuf = offset;
        this.closed = false;
        this.hasLineFeed = false;
    }
    
    @Override
    public int read() throws IOException {
        byte[] buf = new byte[1];
        int result = this.read(buf, 0, 1);
        
        if(result == -1)
            return -1;
        else
            return buf[0];
        
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
                byte[] line = this.buffer.readLineInBytes();
                if(line.length > 2 && line[0] == END[0] && line[1] == END[1] && line[2] == END[2]){
                    this.closed = true;
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
                offset += maxRead;
            }
            else{
                System.arraycopy(
                    this.byteBuffer, 
                    this.offsetBuf, 
                    bytes, 
                    offset, 
                    maxWrite);
                
                this.offsetBuf += maxWrite;
                offset += maxWrite;
            }            
        }
        
        return offset - initOffset;
    }
    
    @Override
    public void close() throws IOException{
        if(this.closed)
            return;
        
        byte[] line;
        while((line = this.buffer.readLineInBytes()) != null){

            if(line.length > 2 && line[0] == END[0] && line[1] == END[1] && line[2] == END[2]){
                this.closed = true;
                break;
            }            
        }
            
    }
}
