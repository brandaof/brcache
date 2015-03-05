/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;

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
    
    public TextInputStreamReader(StringBufferReader buffer, int offset){
        this.buffer = buffer;
        this.byteBuffer = null;
        this.offsetBuf = offset;
        this.closed = false;
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
    
    public int read(byte[] bytes, int start, int offset) throws IOException {    
        
        if(closed)
            return -1;
        
        int limitRead = offset - start;
        int read = 0;
        boolean linefeed = false;
        boolean insertlinefeed = false;
        
        while(!closed && read < limitRead){
            
            int maxRead  = byteBuffer == null? 0 : byteBuffer.length - this.offsetBuf;
            int maxWrite = limitRead - read;

            if(maxRead == 0){
                byte[] line = this.buffer.readLineInBytes();
                if(line.length > 2 && line[0] == END[0] && line[1] == END[1] && line[2] == END[2]){
                    this.closed = true;
                    return read;
                }
                
                insertlinefeed = linefeed;
                
                this.byteBuffer = line;
                this.offsetBuf = 0;
                maxRead = (insertlinefeed? 2 : 0) + byteBuffer.length - this.offsetBuf;
                linefeed = this.buffer.isHasLineFeed();
            }
            
            if(maxWrite > maxRead){
                
                if(insertlinefeed)
                    System.arraycopy(CRLF, 0, bytes, read, CRLF.length);
                
                System.arraycopy(
                        this.byteBuffer, 
                        this.offsetBuf, 
                        bytes, 
                        insertlinefeed? read + 2 : read, 
                        insertlinefeed? maxRead - 2 : maxRead);
                
                this.offsetBuf += maxRead;
                read += maxRead;
            }
            else{
                if(insertlinefeed)
                    System.arraycopy(CRLF, 0, bytes, read, CRLF.length);
                
                System.arraycopy(
                    this.byteBuffer, 
                    this.offsetBuf, 
                    bytes, 
                    insertlinefeed? read + 2 : read, 
                    insertlinefeed? maxWrite - 2 : maxWrite);
                
                this.offsetBuf += maxWrite;
                read += maxWrite;
            }            
        }
        
        return read;
    }
    
    @Override
    public void close() throws IOException{
        if(this.closed)
            return;
        
        StringBuilder line;
        while((line = this.buffer.readLine()) != null){

            if(line.length() > 2 && line.substring(0, 3).equals(END)){
                this.closed = true;
                break;
            }            
        }
            
    }
}
