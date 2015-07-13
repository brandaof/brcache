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
import java.util.LinkedHashMap;
import java.util.Map;

import org.brandao.brcache.Cache;
import org.brandao.brcache.CacheInputStream;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.server.command.ExitCommand;
import org.brandao.brcache.server.command.GetCommand;
import org.brandao.brcache.server.command.PutCommand;
import org.brandao.brcache.server.command.RemoveCommand;
import org.brandao.brcache.server.command.StatsCommand;

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
    
    private byte[] buffer;
    
    private Map<String, Command> commands;
    
    public Terminal(Configuration config){
        this.run = false;
        this.config = config;
    }

    protected void init(Socket socket, Cache cache, 
            StreamFactory streamFactory,
            int readBufferSize, int writeBufferSize) throws IOException{
        try{
        	this.commands = new LinkedHashMap<String, Command>();
        	
        	Command cmd = new PutCommand();
        	cmd.setTerminal(this);
        	this.commands.put("PUT", cmd);
        	
        	cmd = new GetCommand();
        	cmd.setTerminal(this);
        	this.commands.put("GET", cmd);
        	
        	cmd = new RemoveCommand();
        	cmd.setTerminal(this);
        	this.commands.put("REMOVE", cmd);
        	
        	cmd = new StatsCommand();
        	cmd.setTerminal(this);
        	this.commands.put("STATS", cmd);
        	
        	cmd = new ExitCommand();
        	cmd.setTerminal(this);
        	this.commands.put("EXIT", new ExitCommand());
        	
            this.socket = socket;
            this.cache  = cache;
            this.readBufferSize  = readBufferSize;
            this.writeBufferSize = writeBufferSize;
            this.reader = new TextTerminalReader(this.socket, streamFactory, readBufferSize);
            this.writer = new TextTerminalWriter(this.socket, streamFactory, writeBufferSize);
            this.buffer = new byte[this.readBufferSize];
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
    
    public Configuration getConfiguration() {
		return config;
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
                this.executeCommand();
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
        
        StringBuilder message = reader.getMessage();
        String[] command = message.toString().split(" ");
        
        if(command.length < 1)
        	throw new UnknowCommandException(String.format(TerminalConstants.UNKNOW_COMMAND, "empty"));
        
        String[] parameters;
        if(command.length < 2)
        	parameters = TerminalConstants.EMPTY_STR_ARRAY;
        else{
        	parameters = new String[command.length - 1];
        	System.arraycopy(command, 1, parameters, 0, parameters.length);
        }
        
        Command cmd = this.commands.get(command[0]);
        
        if(cmd == null)
        	throw new UnknowCommandException(String.format(TerminalConstants.UNKNOW_COMMAND, command[0]));
        
        cmd.execute(cache, reader, writer, parameters);
    }
    
}
