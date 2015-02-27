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
public class Terminal implements Runnable{
    
    private static final byte[] CRLF = "\r\n".getBytes();
    
    private Cache cache;
    
    private Socket socket;
    
    private final ServerSocket serverSocket;
    
    private boolean run;
    
    private TerminalReader reader;
    
    private TerminalWriter writer;
    
    public Terminal(ServerSocket serverSocket, Cache cache){
        this.serverSocket = serverSocket;
        this.cache = cache;
        this.run = false;
    }

    public void init() throws IOException{
        try{
            this.socket = this.serverSocket.accept();
            this.reader = new TextTerminalReader(this.socket);
            this.writer = new TextTerminalWriter(this.socket);
            this.run = true;
        }
        catch(Throwable e){
            if(this.socket != null)
                this.socket.close();
        }
        finally{
            if(this.socket == null || this.socket.isClosed())
                this.run = false;
        }
    }
    
    public void run(){
        while(this.run){
            try{
                execute();
            }
            catch (UnknowCommandException ex) {
                try{
                    this.writer.sendMessage("UNKNOW COMMAND: " + ex.getMessage());
                }
                catch(Exception e){
                }
            }
            catch (ReadDataException ex) {
                try{
                    this.writer.sendMessage("READ DATA ERROR: " + ex.getMessage());
                }
                catch(Exception e){
                }
            }
            catch (WriteDataException ex) {
                try{
                    this.writer.sendMessage("WRITE DATA ERROR: " + ex.getMessage());
                }
                catch(Exception e){
                }
            }
            catch (ParameterException ex) {
                try{
                    this.writer.sendMessage(ex.getMessage());
                }
                catch(Exception e){
                }
            }
            catch(Throwable ex){
                try{
                    this.writer.sendMessage("unknow error");
                }
                catch(Exception e){
                }
                ex.printStackTrace();
            }
        }
    }
    
    public void execute() 
        throws UnknowCommandException, ReadDataException, 
        WriteDataException, ParameterException{
        
        Command command = reader.getCommand();
        
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
                throw new UnknowCommandException(command.name());
        }
    }
    
    public void stop() throws IOException{
        this.run = false;
        if(this.socket != null)
            this.socket.close();
    }
    
    private void executePut(TerminalReader reader, TerminalWriter writer) 
            throws ReadDataException, WriteDataException, ParameterException {
        StringBuilder[] parameters = null;
        try{
            parameters = reader.getParameters(2);
            
            if(parameters == null || parameters.length != 2)
                throw new ParameterException("EXPECTED THE KEY AND TIME");
            
            this.cache.put(
                parameters[0].toString(), 
                Integer.parseInt(parameters[1].toString()), 
                reader.getStream());
            
            writer.sendMessage("OK");
        }
        catch(NumberFormatException e){
            throw new ReadDataException("invalid time: " + parameters[1]);
        }
        catch (IOException ex) {
            throw new WriteDataException("insert entry fail");
        }
    }

    private void executeGet(TerminalReader reader, TerminalWriter writer) 
            throws WriteDataException, ReadDataException, ParameterException{
        
        try{
            StringBuilder[] parameters = reader.getParameters(1);
            
            if(parameters == null || parameters.length != 1)
                throw new ParameterException("EXPECTED THE KEY");

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

            writer.sendMessage("");
            writer.sendMessage("END");
        }
        catch(IOException e){
            throw new ReadDataException("read entry fail");
        }
    }

    private void executeRemove(TerminalReader reader, TerminalWriter writer) 
            throws ReadDataException, WriteDataException, ParameterException{
        StringBuilder[] parameters = reader.getParameters(1);

        if(parameters == null || parameters.length != 1)
            throw new ParameterException("EXPECTED THE KEY");

        this.cache.remove(parameters[0].toString());

        writer.sendMessage("END");
    }

    private void executeStats(TerminalReader reader, TerminalWriter writer) 
            throws WriteDataException{
        writer.sendMessage("read entry: " + this.cache.getCountRead());
        writer.sendMessage("read data: " + this.cache.getCountReadData());
        writer.sendMessage("write entry: " + this.cache.getCountWrite());
        writer.sendMessage("write data: " + this.cache.getCountWriteData());
        writer.sendMessage("END");
    }

    private void executeExit(TerminalReader reader, TerminalWriter writer) throws WriteDataException{
        try{
            writer.sendMessage("goodbye!");
            this.socket.close();
        }
        catch(IOException e){
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
    
}
