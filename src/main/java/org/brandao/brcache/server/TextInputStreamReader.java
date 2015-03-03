/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 *
 * @author Cliente
 */
public class TextInputStreamReader extends InputStream{

    private static final String CRLF = "\r\n";
    
    private String END = "END";
    
    private StringBuffer buffer;
    
    private BufferedReader reader;
    
    private byte[] byteBuffer;
    
    private int offsetBuf;
    
    private boolean closed;
    
    public TextInputStreamReader(StringBuffer buffer, int offset, BufferedReader reader){
        this.buffer = buffer;
        this.reader = reader;
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

        while(!closed && read < limitRead){
            
            int maxRead  = byteBuffer == null? 0 : byteBuffer.length - this.offsetBuf;
            int maxWrite = limitRead - read;

            if(maxRead == 0){
                StringBuilder line = this.buffer.readLine();
                if(line.length() > 2 && line.substring(0, 3).equals(END)){
                    this.closed = true;
                    return read;
                }
                
                if(linefeed)
                    line.insert(0, CRLF);
                
                this.byteBuffer = line.toString().getBytes();
                this.byteBuffer = Arrays.copyOf(this.byteBuffer, this.byteBuffer.length);
                this.offsetBuf = 0;
                maxRead = byteBuffer.length - this.offsetBuf;
                linefeed = this.buffer.isHasLineFeed();
            }
            
            if(maxWrite > maxRead){
                System.arraycopy(this.byteBuffer, this.offsetBuf, bytes, read, maxRead);
                this.offsetBuf += maxRead;
                read += maxRead;
            }
            else{
                System.arraycopy(this.byteBuffer, this.offsetBuf, bytes, read, maxWrite);
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
