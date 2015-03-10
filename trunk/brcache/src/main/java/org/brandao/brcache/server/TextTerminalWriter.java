/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 *
 * @author Cliente
 */
public class TextTerminalWriter implements TerminalWriter{

    private TextBufferWriter buffer;
    
    private int writeBufferSize;
    
    public TextTerminalWriter(Socket socket, int writeBufferSize) throws IOException{
        this.writeBufferSize = writeBufferSize;
        this.buffer = new TextBufferWriter(this.writeBufferSize, socket.getOutputStream());
    }

    public void sendMessage(String message) throws WriteDataException {
        try{
            this.buffer.write(message.getBytes());
            this.buffer.write(TerminalConstants.CRLF);
        }
        catch(IOException e){
            throw new WriteDataException(TerminalConstants.SEND_MESSAGE_FAIL, e);
        }
    }

    public void sendCRLF() throws WriteDataException {
        try{
            this.buffer.write(TerminalConstants.CRLF);
        }
        catch(IOException e){
            throw new WriteDataException(TerminalConstants.SEND_MESSAGE_FAIL, e);
        }
            
    }

    public void flush() throws WriteDataException {
        try{
            this.buffer.flush();
        }
        catch(IOException e){
            throw new WriteDataException(TerminalConstants.SEND_MESSAGE_FAIL, e);
        }
    }
    
    public OutputStream getStream() {
        return new TextOutputStream(this.buffer);
    }
    
}
