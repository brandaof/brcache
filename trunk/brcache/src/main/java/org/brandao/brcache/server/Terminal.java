/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import org.brandao.brcache.Cache;

/**
 *
 * @author Cliente
 */
public class Terminal {
    
    private final Cache cache;
    
    private final Socket socket;
    
    private boolean run;
    
    public Terminal(Socket socket, Cache cache){
        this.socket = socket;
        this.cache = cache;
        this.run = false;
    }

    public void start() throws IOException{
        TerminalReader reader = new TextTerminalReader(socket);
        TerminalWriter writer = new TextTerminalWriter(socket);
        this.run = true;
        Command command;
        while(this.run){
            try{
                command = reader.getCommand();
                
                switch(command){
                    case PUT:
                        this.executePut(reader, writer);
                        break;
                    case GET:
                        this.executeGet(reader, writer);
                        break;
                    case REMOVE:
                        this.executeRemove(reader, writer);
                        break;
                    case STATS:
                        this.executeStats(reader, writer);
                        break;
                    case EXIT:
                        this.executeExit(reader, writer);
                        break;
                    default:
                        writer.sendMessage("ERROR: UNKNOW COMMAND");
                };
            }
            catch(Exception e){
                if(this.socket.isClosed())
                    this.run = false;
                else
                    writer.sendMessage(e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    private void executePut(TerminalReader reader, TerminalWriter writer) throws IOException{
        StringBuilder[] parameters = null;
        try{
            parameters = reader.getParameters(2);
            
            if(parameters == null || parameters.length != 2){
                writer.sendMessage("ERROR: EXPECTED THE KEY AND TIME");
                return;
            }
            
            this.cache.put(
                parameters[0].toString(), 
                Integer.parseInt(parameters[1].toString()), 
                reader.getStream());
            
            writer.sendMessage("OK");
        }
        catch(NumberFormatException e){
            throw new IOException("invalid time: " + parameters[1]);
        }
        catch(Throwable e){
            e.printStackTrace();
            writer.sendMessage("ERROR");
        }
    }

    private void executeGet(TerminalReader reader, TerminalWriter writer) throws IOException{
        try{
            StringBuilder[] parameters = reader.getParameters(1);
            if(parameters == null || parameters.length != 1){
                writer.sendMessage("ERROR: EXPECTED THE KEY");
                return;
            }
            
            String key = parameters[0].toString();
            InputStream in = this.cache.get(key);
            
            if(in != null){
                OutputStream out = writer.getStream();
                byte[] buffer = new byte[2048];
                int len;
                while((len = in.read(buffer)) != -1){
                    out.write(buffer, 0, len);
                }
            }
            
            writer.sendMessage("END");
        }
        catch(Throwable e){
            e.printStackTrace();
            writer.sendMessage("");
            writer.sendMessage("ERROR: " + e.getMessage());
        }
    }

    private void executeRemove(TerminalReader reader, TerminalWriter writer) throws IOException{
        try{
            StringBuilder[] parameters = reader.getParameters(1);
            
            if(parameters == null || parameters.length != 1){
                writer.sendMessage("ERROR: EXPECTED THE KEY");
                return;
            }
            
            this.cache.remove(parameters[0].toString());
            
            writer.sendMessage("END");
        }
        catch(Throwable e){
            e.printStackTrace();
            writer.sendMessage("ERROR");
        }
    }

    private void executeStats(TerminalReader reader, TerminalWriter writer) throws IOException{
        try{
            writer.sendMessage("read entry: " + this.cache.getCountRead());
            writer.sendMessage("read data: " + this.cache.getCountReadData());
            writer.sendMessage("write entry: " + this.cache.getCountWrite());
            writer.sendMessage("write data: " + this.cache.getCountWriteData());
            writer.sendMessage("END");
        }
        catch(Throwable e){
            e.printStackTrace();
            writer.sendMessage("ERROR");
        }
    }

    private void executeExit(TerminalReader reader, TerminalWriter writer) throws IOException{
        try{
            writer.sendMessage("goodbye!");
            this.socket.close();
        }
        finally{
            this.run = false;
        }
    }
    
    public Socket getSocket() {
        return socket;
    }

    public Cache getCache() {
        return cache;
    }

    public static void main(String[] ss) throws IOException{
        
        ServerSocket s = new ServerSocket(1044);
        while(true){
            Socket sock = s.accept();
            Terminal t = new Terminal(sock, new Cache());
            t.start();
        }
    }
}
