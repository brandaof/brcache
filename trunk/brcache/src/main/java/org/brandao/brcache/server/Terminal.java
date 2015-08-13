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
import java.net.Socket;

import org.brandao.brcache.Cache;
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
    
	private static final Command PUT    = new PutCommand();

	private static final Command GET    = new GetCommand();

	private static final Command REMOVE = new RemoveCommand();

	private static final Command STATS  = new StatsCommand();
	
	private static final Command EXIT   = new ExitCommand();
	
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
        while(this.run){
            try{
                String message = reader.getMessage();
                String[] command = message.split(" ");
                
            	if("GET".equals(command[0]))
            		GET.execute(cache, reader, writer, command);
            	else
            	if("PUT".equals(command[0]))
            		PUT.execute(cache, reader, writer, command);
            	else
            	if("REMOVE".equals(command[0]))
            		REMOVE.execute(cache, reader, writer, command);
            	else
            	if("STATS".equals(command[0]))
            		STATS.execute(cache, reader, writer, command);
            	else
            	if("EXIT".equals(command[0]))
            		EXIT.execute(cache, reader, writer, command);
            	else
                	throw new UnknowCommandException(String.format(TerminalConstants.UNKNOW_COMMAND, command[0]));
            }
            catch (IndexOutOfBoundsException ex) {
                this.writer.sendMessage(String.format(TerminalConstants.UNKNOW_COMMAND, "empty"));
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
    
}
