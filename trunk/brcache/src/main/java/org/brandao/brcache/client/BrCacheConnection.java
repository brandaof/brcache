/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.TextTerminalReader;
import org.brandao.brcache.server.TextTerminalWriter;

/**
 *
 * @author Cliente
 */
public class BrCacheConnection {
    
    public static final String CRLF   = "\r\n";
    
    public static final String END    = "END";

    public static final String PUT    = "PUT";

    public static final String GET    = "GET";
    
    public static final String REMOVE = "REMOVE";
    
    public static final String OK     = "OK";
    
    private String host;
    
    private int port;
    
    private Socket socket;
    
    private TerminalReader reader;
    
    private TerminalWriter writer;
    
    private BrCacheConnection(String host, int port){
        this.host = host;
        this.port = port;
    }
    
    public void connect() throws IOException{
        this.socket = new Socket(this.getHost(), this.getPort());
        this.reader = new TextTerminalReader(this.socket);
        this.writer = new TextTerminalWriter(this.socket);
    }

    public void disconect() throws IOException{
        
        if(this.socket != null)
            this.socket.close();
        
        this.reader = null;
        this.writer = null;
    }
    
    public void put(String key, long time, Object value) throws IOException{
        this.writer.sendMessage(PUT);
        this.writer.sendMessage(key);
        this.writer.sendMessage(String.valueOf(time));
        
        ObjectOutputStream out = null;
        try{
            out = new ObjectOutputStream(this.writer.getStream());
            out.writeObject(value);
            out.flush();
        }
        finally{
            if(out != null)
                out.close();
        }
        
        this.writer.sendMessage("");
        this.writer.sendMessage(END);
        
        StringBuilder[] result = this.reader.getParameters(1);
        
        if(!result[0].toString().equals(OK))
            throw new IOException(result[0].toString());
    }
    
    public Object get(String key) throws IOException, ClassNotFoundException{
        this.writer.sendMessage(GET);
        this.writer.sendMessage(key);
        
        ObjectInputStream stream = null;
        try{
            stream = new ObjectInputStream(this.reader.getStream());
            return stream.readObject();
        }
        finally{
            if(stream != null)
                stream.close();
        }
    }

    public void remove(String key) throws IOException, ClassNotFoundException{
        this.writer.sendMessage(REMOVE);
        this.writer.sendMessage(key);
        StringBuilder[] response = this.reader.getParameters(1);
        
        if(!"OK".equals(response.toString()))
            throw new IOException(response.toString());
    }
    
    
    public static void main(String[] aaa) throws IOException, ClassNotFoundException{
        BrCacheConnection c = new BrCacheConnection("localhost", 1044);
        c.connect();
        c.put("TT", 0, "TESTE \n TESTE \n");
        
        String s = (String) c.get("TT");
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
