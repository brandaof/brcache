/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.brandao.brcache.Cache;
import org.brandao.brcache.StorageException;

/**
 *
 * @author Cliente
 */
public class Terminal {
    
    private static final byte[] CRLF = "\r\n".getBytes();
    
    private Cache cache;
    
    private Socket socket;
    
    private boolean run;
    
    private TerminalReader reader;
    
    private TerminalWriter writer;
    
    private Configuration config;
    
    public Terminal(Configuration config){
        this.run = false;
        this.config = config;
    }

    protected void init(Socket socket, Cache cache, int readBufferSize) throws IOException{
        try{
            this.socket = socket;
            this.cache  = cache;
            this.reader = new TextTerminalReader(this.socket, readBufferSize);
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
    
    public void destroy() throws IOException{
        try{
            if(this.socket != null)
                this.socket.close();
        }
        finally{
            this.run    = false;
            this.cache  = null;
            this.reader = null;
            this.writer = null;
        }
    }
    
    public void execute() throws WriteDataException{
        while(this.run && !this.socket.isInputShutdown()){
            
            try{
                executeCommand();
            }
            catch (UnknowCommandException ex) {
                ex.printStackTrace();
                this.writer.sendMessage("UNKNOW COMMAND: " + ex.getMessage());
            }
            catch (ReadDataException ex) {
                ex.printStackTrace();
                this.writer.sendMessage("READ DATA ERROR: " + ex.getMessage());
            }
            catch (WriteDataException ex) {
                ex.printStackTrace();
                this.writer.sendMessage("WRITE DATA ERROR: " + ex.getMessage());
            }
            catch (ParameterException ex) {
                ex.printStackTrace();
                this.writer.sendMessage(ex.getMessage());
            }
            catch(Throwable ex){
                ex.printStackTrace();
                this.writer.sendMessage("unknow error");
            }
        }
    }
    
    public void executeCommand() 
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
            writer.flush();
        }
        catch(NumberFormatException e){
            throw new ReadDataException("invalid time: " + parameters[1], e);
        }
        catch (IOException ex) {
            throw new WriteDataException("insert entry fail", ex);
        }
        catch(StorageException ex){
            throw new WriteDataException("insert entry fail", ex);
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
            writer.flush();
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
        writer.flush();
    }

    private void executeStats(TerminalReader reader, TerminalWriter writer) 
            throws WriteDataException{
        for(String prop: this.config.stringPropertyNames())
            writer.sendMessage(prop + ": " + this.config.getProperty(prop));
        
        writer.sendMessage("read_entry: " + this.cache.getCountRead());
        writer.sendMessage("read_data: " + this.cache.getCountReadData());
        writer.sendMessage("write_entry: " + this.cache.getCountWrite());
        writer.sendMessage("write_data: " + this.cache.getCountWriteData());
        writer.sendMessage("END");
        writer.flush();
    }

    private void executeExit(TerminalReader reader, TerminalWriter writer) throws WriteDataException{
        try{
            writer.sendMessage("goodbye!");
            writer.flush();
            this.socket.close();
        }
        catch(IOException e){
        }
        finally{
            this.run = false;
        }
    }
    
}
