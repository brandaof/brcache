/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 *
 * @author Cliente
 */
public class TextTerminalReader implements TerminalReader{

    private Socket socket;
    
    private BufferedReader reader;
    
    private StringBuilder buffer;
    
    public TextTerminalReader(Socket socket) throws IOException{
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.buffer = new StringBuilder();
    }
    
    public Command getCommand() throws IOException {
        StringBuilder line = this.readLine();
        try{
            return Command.valueOf(line.toString().trim());
        }
        catch(Exception e){
            throw new IOException("UNKNOW COMMAND: " + line);
        }
    }

    public StringBuilder[] getParameters(int size) throws IOException {
        StringBuilder[] params = new StringBuilder[size];
        
        for(int i=0;i<size;i++)
            params[i] = readLine();
        
        return params;
    }

    public InputStream getStream() {
        return new TextInputStreamReader(buffer, reader);
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
