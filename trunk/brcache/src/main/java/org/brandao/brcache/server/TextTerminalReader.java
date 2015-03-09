/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/**
 *
 * @author Cliente
 */
public class TextTerminalReader implements TerminalReader{

    private Socket socket;
    
    private InputStream stream;
    
    private StringBufferReader buffer;
    
    private int offset;
    
    public TextTerminalReader(Socket socket, int readBufferSize) throws IOException{
        this.socket = socket;
        this.stream = socket.getInputStream();
        this.buffer = new StringBufferReader(readBufferSize, this.stream);
        this.offset = 0;
    }
    
    public Command getCommand() throws UnknowCommandException, ReadDataException {
        StringBuilder line = null;
        try{
            line = this.buffer.readLine();
            return Command.valueOf(line.toString());
        }
        catch(IOException e){
            throw new ReadDataException("can not read command: " + line);
        }
        catch(NullPointerException e){
            throw new UnknowCommandException("undefined");
        }
        catch(IllegalArgumentException e){
            throw new UnknowCommandException(line == null? "undefined" : line.toString());
        }
    }

    public StringBuilder[] getParameters(int size) throws ReadDataException, ParameterException {
        StringBuilder[] params = new StringBuilder[size];
        
        for(int i=0;i<size;i++){
            try{
                params[i] = this.buffer.readLine();
                if(params[i] == null)
                    throw new ParameterException("empty parameter: " + i);
            }
            catch(IOException e){
                throw new ReadDataException("can not read parameter: " + i);
            }
        }
        
        return params;
    }

    public InputStream getStream() {
        return new TextInputStreamReader(buffer, this.offset);
    }

    public int getOffset() {
        return offset;
    }
    
}
