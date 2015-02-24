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

    private OutputStream out;
    
    public TextTerminalWriter(Socket socket) throws IOException{
        this.out = socket.getOutputStream();
    }
    
    public void sendMessage(String message) throws IOException {
        out.write((message + "\r\n").getBytes());
        out.flush();
    }

    public OutputStream getStream() {
        return new TextOutputStream(out);
    }
    
}
