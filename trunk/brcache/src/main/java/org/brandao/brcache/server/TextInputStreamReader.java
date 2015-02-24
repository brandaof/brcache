/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

/**
 *
 * @author Cliente
 */
public class TextInputStreamReader extends InputStream{
    
    private StringBuilder END = new StringBuilder("END");
    
    private StringBuilder buffer;
    
    private BufferedReader reader;
    
    private byte[] byteBuffer;
    
    private int pos;
    
    private boolean closed;
    
    public TextInputStreamReader(StringBuilder buffer, BufferedReader reader){
        this.buffer = buffer;
        this.reader = reader;
        this.byteBuffer = null;
        this.pos = 0;
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
        
        while(read < limitRead){
            
            int maxRead  = byteBuffer == null? 0 : byteBuffer.length - pos;
            int maxWrite = limitRead - read;

            if(maxRead == 0){
                
                StringBuilder line = this.readLine();
                String srt = line.toString();
                
                if(END.toString().equals(srt)){
                    this.closed = true;
                    return read;
                }
                
                this.byteBuffer = srt.getBytes();
                this.pos = 0;
                maxRead = byteBuffer.length - pos;
            }
            
            if(maxWrite > maxRead){
                System.arraycopy(this.byteBuffer, pos, bytes, read, maxRead);
                pos += maxRead;
                read += maxRead;
            }
            else{
                System.arraycopy(this.byteBuffer, pos, bytes, read, maxWrite);
                pos += maxWrite;
                read += maxWrite;
            }            
        }
        
        return read;
    }
    
    protected StringBuilder readLine() throws IOException{
        char[] tmp = new char[2048];
        int start  = this.buffer.length();
        int pos    = start;
        int end    = -1;
        while(end == -1){
            
            if(pos >= this.buffer.length()){
                int len = reader.read(tmp);
                this.buffer.append(tmp, 0, len);
            }
            
            if(this.buffer.charAt(pos) == '\r' || this.buffer.length() > 2048)
                end = pos;
            else
                pos++;
        }
        
        StringBuilder result = new StringBuilder(this.buffer.subSequence(start, end));
        
        this.buffer.delete(0, buffer.length());
        return result;
    }
    
}
