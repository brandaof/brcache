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

    private static final byte[] CRLF = "\r\n".getBytes();
    
    private OutputStream out;
    
    public TextTerminalWriter(Socket socket) throws IOException{
        this.out = socket.getOutputStream();
    }

    public void sendMessage(String message) throws WriteDataException {
        try{
            out.write(message.getBytes());
            out.write(CRLF);
            out.flush();
        }
        catch(IOException e){
            throw new WriteDataException("send message fail", e);
        }
    }

    public void sendCRLF() throws WriteDataException {
        try{
            out.write(CRLF);
            out.flush();
        }
        catch(IOException e){
            throw new WriteDataException("send CRLF fail", e);
        }
            
    }

    public void flush() throws WriteDataException {
        try{
            out.flush();
        }
        catch(IOException e){
            throw new WriteDataException("send message fail", e);
        }
    }
    
    public OutputStream getStream() {
        return new TextOutputStream(out);
    }
    
}
