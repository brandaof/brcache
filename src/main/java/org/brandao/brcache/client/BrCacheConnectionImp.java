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
    
    public static final String PUT_COMMAND               = "PUT";

    public static final String ERROR                     = "ERROR";
    
    public static final String GET_COMMAND               = "GET";
    
    public static final String REMOVE_COMMAND            = "REMOVE";

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
        
        try{
        	String cmd = 
        			PUT_COMMAND + SEPARATOR_COMMAND +
        			key + SEPARATOR_COMMAND +
        			time + SEPARATOR_COMMAND;

            ObjectOutputStream out     = null;
        	ByteArrayOutputStream bout = null;
            try{
            	bout = new ByteArrayOutputStream();
                out = new ObjectOutputStream(bout);
                out.writeObject(value);
                out.flush();
                
                byte[] stream = bout.toByteArray();
                int size      = stream.length;
                
                cmd += size;
                this.writer.sendMessage(cmd);
                this.writer.getStream().write(stream);
                this.writer.sendCRLF();
                this.writer.sendMessage(BOUNDARY);
            }
            catch(IOException ex){
                this.writer.sendCRLF();
                this.writer.sendMessage(ERROR);
                throw new StorageException("send entry fail: " + key, ex);
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
                this.writer.flush();
            }


            StringBuilder result = this.reader.getMessage();

            String resultSTR = result.toString();

            if(!resultSTR.equals(SUCCESS))
                throw new StorageException(resultSTR);
        }
        catch(StorageException e){
            throw e;
        } catch (ReadDataException e) {
            throw new StorageException(e);
        } catch (WriteDataException e) {
            throw new StorageException(e);
        }
    }
    
    public synchronized Object get(String key) throws RecoverException{
        try{
        	String cmd =
        			GET_COMMAND + SEPARATOR_COMMAND +
        			key;

        	this.writer.sendMessage(cmd);
        	this.writer.flush();
        	
            StringBuilder result = this.reader.getMessage();
            String[] resultParams = result.toString().split(" ");
            
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
            catch(EOFException ex){
                return null;
            }
            catch(IOException ex){
                throw new RecoverException("read entry fail: " + key, ex);
            }
            catch(ClassNotFoundException ex){
                throw new RecoverException("create instance fail: " + key, ex);
            }
            finally{
                if(in != null){
                    try{
                        in.close();
                    }
                    catch(Exception e){}
                }
                
                StringBuilder end = this.reader.getMessage();
                
                if(!BOUNDARY.equals(end.toString()))
                    throw new RecoverException("read entry fail");
            }
        }
        catch (ReadDataException e) {
            throw new RecoverException(e);
        }
        catch(RecoverException e){
            throw e;
        } catch (WriteDataException e) {
            throw new RecoverException(e);
        }
    }

    public synchronized boolean remove(String key) throws RecoverException{
        
        try{
        	String cmd = 
        			REMOVE_COMMAND + SEPARATOR_COMMAND +
        			key;

        	this.writer.sendMessage(cmd);
        	this.writer.flush();

            StringBuilder response = this.reader.getMessage();
            String responseSTR = response.toString();
            
            if(!SUCCESS.equals(responseSTR))
                throw new RecoverException(responseSTR);

            return true;
        }
        catch(RecoverException e){
            throw e;
        } catch (ReadDataException e) {
            throw new RecoverException(e);
        } catch (WriteDataException e) {
            throw new RecoverException(e);
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
