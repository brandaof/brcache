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

package org.brandao.brcache.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.server.DefaultStreamFactory;
import org.brandao.brcache.server.LimitedTextInputStreamReader;
import org.brandao.brcache.server.ReadDataException;
import org.brandao.brcache.server.StreamFactory;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.TextTerminalReader;
import org.brandao.brcache.server.TextTerminalWriter;
import org.brandao.brcache.server.WriteDataException;

/**
 * Permite o armazenamento, atualização, remoção de um item em um servidor BrCache.
 * 
 * @author Brandao.
 */
public class BrCacheConnectionImp implements BrCacheConnection{
    
    public static final String CRLF                      = "\r\n";
    
    public static final String BOUNDARY                  = "END";
    
    public static final String PUT_COMMAND               = "put";

    public static final String ERROR                     = "ERROR";
    
    public static final String GET_COMMAND               = "get";
    
    public static final String REMOVE_COMMAND            = "remove";

    public static final String VALUE_RESULT              = "VALUE";
    
    public static final String SUCCESS                   = "OK";

    public static final String SEPARATOR_COMMAND         = " ";
    
    private String host;
    
    private int port;
    
    private Socket socket;
    
    private TerminalReader reader;
    
    private TerminalWriter writer;
    
    private StreamFactory streamFactory;

    public BrCacheConnectionImp(String host, int port){
        this(host, port, new DefaultStreamFactory());
    }
    
    /**
     * Cria uma nova instância de {@link BrCacheConnection}
     * @param host Endereço do servidor.
     * @param port Porta que o servidor está escutando.
     */
    public BrCacheConnectionImp(String host, int port, StreamFactory streamFactory){
        this.host = host;
        this.port = port;
        this.streamFactory = streamFactory;
    }
    
    public synchronized void connect() throws IOException{
        this.socket = new Socket(this.getHost(), this.getPort());
        this.reader = new TextTerminalReader(this.socket, this.streamFactory, 8*1024);
        this.writer = new TextTerminalWriter(this.socket, this.streamFactory, 8*1024);
    }
    
    public synchronized void disconect() throws IOException{
        
        if(this.socket != null)
            this.socket.close();
        
        this.reader = null;
        this.writer = null;
    }
    
    public synchronized void put(String key, long time, Object value) 
            throws StorageException{

    	String cmd = 
    			PUT_COMMAND + SEPARATOR_COMMAND +
    			key + SEPARATOR_COMMAND +
    			time + SEPARATOR_COMMAND;

    	byte[] data = null;
    	
        ObjectOutputStream out     = null;
    	ByteArrayOutputStream bout = null;
    	
        try{
        	bout = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bout);
            out.writeObject(value);
            out.flush();
            
            data     = bout.toByteArray();
            int size = data.length;
            
            cmd += size;
        }
        catch(IOException ex){
            throw new StorageException("serializable data fail: " + key);
        }
        finally{
            try{
                if(bout != null)
                	bout.close();
                
                if(out != null)
                	out.close();
            }
            catch(Exception ex){
            }
        }

        try{
            this.writer.sendMessage(cmd);
            this.writer.getStream().write(data);
            this.writer.sendCRLF();
            this.writer.sendMessage(BOUNDARY);
            this.writer.flush();
        }
        catch(WriteDataException ex){
        	if(ex.getCause() instanceof IOException && !"premature end of data".equals(ex.getCause().getMessage()))
                throw new StorageException("send data fail: " + ex.getMessage(), ex);
        	else
        		throw new StorageException("send data fail: " + ex.getMessage());
        }
        catch(IOException e){
            throw new StorageException("send data fail: " + e.getMessage(), e);
        }
        
        
        try{
            String result = this.reader.getMessage();

            if(!result.equals(SUCCESS))
                throw new StorageException(result);
        }
        catch (ReadDataException ex) {
        	if(ex.getCause() instanceof IOException && !"premature end of data".equals(ex.getCause().getMessage()))
                throw new StorageException("read data fail: " + ex.getMessage(), ex);
        	else
        		throw new StorageException("read data fail: " + ex.getMessage());
		}
        
    }
    
    public synchronized Object get(String key) throws RecoverException{
    	String cmd =
    			GET_COMMAND + SEPARATOR_COMMAND +
    			key;
    	
    	try{
        	this.writer.sendMessage(cmd);
        	this.writer.flush();
    	}
    	catch(WriteDataException e){
            throw new RecoverException("send data fail: " + key, e.getCause());
    	}
    	
    	try{
            String result = this.reader.getMessage();
            
            if(!result.startsWith("VALUE"))
            	throw new ReadDataException(result);
            
            String[] resultParams = result.split(" ");
            
            if(resultParams.length != 4 || !VALUE_RESULT.equals(resultParams[0]))
                throw new RecoverException(result.toString());

            int size = Integer.parseInt(resultParams[2]);
    		
            LimitedTextInputStreamReader in = null;
            
            try{
	        	if(size != 0){
	            	in = (LimitedTextInputStreamReader) this.reader.getStream(size);
	            	byte[] buffer = in.read(size);
	                ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(buffer));
	                return stream.readObject();
	        	}
	        	else
	        		return null;
            }
            finally{
                if(in != null){
                    try{
                        in.close();
                    }
                    catch(Exception e){}
                }
                
                String end = this.reader.getMessage();
                
                if(!BOUNDARY.equals(end))
                    throw new RecoverException("read entry fail");
            }
            
    	}
        catch (ReadDataException ex) {
        	if(ex.getCause() instanceof IOException && !"premature end of data".equals(ex.getCause().getMessage()))
                throw new RecoverException("read data fail: " + ex.getMessage(), ex);
        	else
        		throw new RecoverException("read data fail: " + ex.getMessage());
		}
        catch (IOException ex) {
        	if(ex.getCause() instanceof IOException && !"premature end of data".equals(ex.getCause().getMessage()))
                throw new RecoverException("read data fail: " + ex.getMessage(), ex);
        	else
        		throw new RecoverException("read data fail: " + ex.getMessage());
		}
        catch(ClassNotFoundException ex){
            throw new RecoverException("create instance fail: " + ex.getMessage(), ex);
        }

    }

    public synchronized boolean remove(String key) throws RecoverException{

    	String cmd = 
    			REMOVE_COMMAND + SEPARATOR_COMMAND +
    			key;

    	try{
        	this.writer.sendMessage(cmd);
        	this.writer.flush();
    	}
    	catch(WriteDataException ex){
        	if(ex.getCause() instanceof IOException && !"premature end of data".equals(ex.getCause().getMessage()))
                throw new RecoverException("read data fail: " + ex.getMessage(), ex);
        	else
        		throw new RecoverException("read data fail: " + ex.getMessage());
    	}
    	
        try{
            String response = this.reader.getMessage();
            
            if(!SUCCESS.equals(response))
                throw new RecoverException(response);

            return true;
        }
        catch (ReadDataException ex) {
        	if(ex.getCause() instanceof IOException && !"premature end of data".equals(ex.getCause().getMessage()))
                throw new RecoverException("read data fail: " + ex.getMessage(), ex);
        	else
        		throw new RecoverException("read data fail: " + ex.getMessage());
        }
    }
    
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void close() throws IOException {
        this.socket.close();
    }

    public StreamFactory getStreamFactory() {
        return streamFactory;
    }
    
}
