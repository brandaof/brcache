/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.brandao.brcache.Cache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

/**
 *
 * @author Brandao
 */
public class Terminal {
    
    private Cache cache;
    
    private Socket socket;
    
    private boolean run;
    
    private TerminalReader reader;
    
    private TerminalWriter writer;
    
    private Configuration config;
    
    private int readBufferSize;
    
    private int writeBufferSize;
    
    public Terminal(Configuration config){
        this.run = false;
        this.config = config;
    }

    protected void init(Socket socket, Cache cache, 
            StreamFactory streamFactory,
            int readBufferSize, int writeBufferSize) throws IOException{
        try{
            this.socket = socket;
            this.cache  = cache;
            this.readBufferSize  = readBufferSize;
            this.writeBufferSize = writeBufferSize;
            this.reader = new TextTerminalReader(this.socket, streamFactory, readBufferSize);
            this.writer = new TextTerminalWriter(this.socket, streamFactory, writeBufferSize);
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
    
    public void execute() throws WriteDataException, ReadDataException, StorageException{
        while(this.run && !this.socket.isInputShutdown()){
            
            try{
                executeCommand();
            }
            catch (UnknowCommandException ex) {
                this.writer.sendMessage(String.format(TerminalConstants.UNKNOW_COMMAND, ex.getMessage()));
                this.writer.flush();
            }
            catch (ReadDataException ex) {
                throw ex;
            }
            catch (WriteDataException ex) {
                throw ex;
            }
            catch (ParameterException ex) {
                this.writer.sendMessage(ex.getMessage());
                this.writer.flush();
            }
            catch(Throwable ex){
                throw new StorageException(ex);
            }
        }
    }
    
    public void executeCommand() 
        throws UnknowCommandException, ReadDataException, 
        WriteDataException, ParameterException, RecoverException{
        
        Command command = reader.getCommand();
        
        switch(command){
            case put:
                this.executePut(reader, writer);
                break;
            case get:
                this.executeGet(reader, writer);
                break;
            case remove:
                this.executeRemove(reader, writer);
                break;
            case stats:
                this.executeStats(reader, writer);
                break;
            case exit:
                this.executeExit(reader, writer);
                break;
            default:
                throw new UnknowCommandException(command.name());
        }
    }
    
    private void executePut(TerminalReader reader, TerminalWriter writer) 
            throws ReadDataException, WriteDataException, ParameterException {
        
        StringBuilder[] parameters;
        int time;
        
        try{
            parameters = reader.getParameters(2);
            
            if(parameters == null || parameters.length != 2)
                throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);
            
            try{
                time = Integer.parseInt(parameters[1].toString());
            }
            catch(NumberFormatException e){
                throw new ParameterException(TerminalConstants.INVALID_TIME);
            }
            
            InputStream stream = null;
            try{
                stream = reader.getStream();
                this.cache.put(
                    parameters[0].toString(), 
                    time, 
                    stream);
            }
            finally{
                if(stream != null)
                    stream.close();
            }
            
            
            writer.sendMessage(TerminalConstants.SUCCESS);
            writer.flush();
        }
        catch (IOException ex) {
            throw new WriteDataException(TerminalConstants.INSERT_ENTRY_FAIL, ex);
        }
        catch(StorageException ex){
            throw new WriteDataException(TerminalConstants.INSERT_ENTRY_FAIL, ex);
        }
    }

    private void executeGet(TerminalReader reader, TerminalWriter writer) 
            throws WriteDataException, ReadDataException, ParameterException, RecoverException{
        
        try{
            StringBuilder[] parameters = reader.getParameters(1);
            
            if(parameters == null || parameters.length != 1)
                throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);

            String key = parameters[0].toString();
            InputStream in = null;

            try{
                in = this.cache.get(key);
                if(in != null){
                    OutputStream out = null;
                    try{
                        out = writer.getStream();
                        byte[] buffer = new byte[8192];
                        int len;
                        while((len = in.read(buffer)) != -1){
                            out.write(buffer, 0, len);
                        }
                    }
                    finally{
                        if(out != null){
                            try{
                                out.close();
                            }
                            catch(Throwable e){
                            }
                        }
                        writer.sendCRLF();
                    }
                }
            }
            finally{
                if(in != null)
                    in.close();
            }

            writer.sendMessage(TerminalConstants.BOUNDARY_MESSAGE);
            writer.flush();
        }
        catch(IOException e){
            throw new ReadDataException(TerminalConstants.READ_ENTRY_FAIL);
        }
    }

    private void executeRemove(TerminalReader reader, TerminalWriter writer) 
            throws ReadDataException, WriteDataException, ParameterException, RecoverException{
        StringBuilder[] parameters = reader.getParameters(1);

        if(parameters == null || parameters.length != 1)
            throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);

        this.cache.remove(parameters[0].toString());

        writer.sendMessage(TerminalConstants.SUCCESS);
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
        writer.sendMessage(TerminalConstants.BOUNDARY_MESSAGE);
        writer.flush();
    }

    private void executeExit(TerminalReader reader, TerminalWriter writer) throws WriteDataException{
        try{
            writer.sendMessage(TerminalConstants.DISCONNECT_MESSAGE);
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
