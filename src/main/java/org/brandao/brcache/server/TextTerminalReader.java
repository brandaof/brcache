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
    
    private StringBuffer buffer;
    
    private int offset;
    
    public TextTerminalReader(Socket socket) throws IOException{
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.buffer = new StringBuffer(2048, this.reader);
        this.offset = 0;
    }
    
    public Command getCommand() throws UnknowCommandException, ReadDataException {
        StringBuilder line = null;
        try{
            line = this.buffer.readLine();
            return Command.valueOf(line.toString());
        }
        catch(IOException e){
            throw new ReadDataException("can not read command!");
        }
        catch(IllegalArgumentException e){
            throw new UnknowCommandException(line == null? "undefined" : line.toString());
        }
    }

    public StringBuilder[] getParameters(int size) throws ReadDataException {
        StringBuilder[] params = new StringBuilder[size];
        
        for(int i=0;i<size;i++){
            try{
                params[i] = this.buffer.readLine();
            }
            catch(IOException e){
                throw new ReadDataException("can not read parameter: " + i);
            }
        }
        
        return params;
    }

    public InputStream getStream() {
        return new TextInputStreamReader(buffer, this.offset, reader);
    }

    public int getOffset() {
        return offset;
    }
    
}
