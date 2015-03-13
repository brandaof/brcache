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

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.TextTerminalReader;
import org.brandao.brcache.server.TextTerminalWriter;

/**
 * Permite o armazenamento, atualização, remoção de um item em um servidor BRCache.
 * 
 * @author Brandao.
 */
public class BrCacheConnection {
    
    public static final String CRLF     = "\r\n";
    
    public static final String BOUNDARY = "end";

    public static final String PUT      = "put";

    public static final String GET      = "get";
    
    public static final String REMOVE   = "remove";
    
    public static final String SUCCESS  = "ok";
    
    private String host;
    
    private int port;
    
    private Socket socket;
    
    private TerminalReader reader;
    
    private TerminalWriter writer;
    
    /**
     * Cria uma nova instância de {@link BrCacheConnection}
     * @param host Endereço do servidor.
     * @param port Porta que o servidor está escutando.
     */
    public BrCacheConnection(String host, int port){
        this.host = host;
        this.port = port;
    }
    
    /**
     * Faz a conexão com o servidor.
     * 
     * @throws IOException Lançada caso ocorra alguma falha ao tentar se
     * conectar ao servidor.
     */
    public synchronized void connect() throws IOException{
        this.socket = new Socket(this.getHost(), this.getPort());
        this.reader = new TextTerminalReader(this.socket, 1*1024*1024);
        this.writer = new TextTerminalWriter(this.socket, 1*1024*1024);
    }
    
    /**
     * Fecha a conexão com o servidor.
     * 
     * @throws IOException Lançada caso ocorra alguma falha ao tentar se
     * fechar a conexão com o servidor.
     */
    public synchronized void disconect() throws IOException{
        
        if(this.socket != null)
            this.socket.close();
        
        this.reader = null;
        this.writer = null;
    }
    
    /**
     * Inclui ou sobrescreve um objeto no cache.
     * 
     * @param key Identificação do objeto no cache.
     * @param time Tempo máximo em milesegundos que o objeto ficará no cache.
     * @param value Objeto a ser incluído no cache.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * objeto no cache.
     */
    public synchronized void put(String key, long time, Object value) 
            throws StorageException{
        
        try{
            this.writer.sendMessage(PUT);
            this.writer.sendMessage(key);
            this.writer.sendMessage(String.valueOf(time));

            ObjectOutputStream out = null;
            try{
                out = new ObjectOutputStream(this.writer.getStream());
                out.writeObject(value);
                out.flush();
            }
            catch(IOException ex){
                throw new StorageException("send entry fail: " + key, ex);
            }
            finally{
                if(out != null){
                    try{
                        out.close();
                    }
                    catch(Exception ex){}
                }
                this.writer.sendCRLF();
                this.writer.sendMessage(BOUNDARY);
                this.writer.flush();
            }


            StringBuilder[] result = this.reader.getParameters(1);

            String resultSTR = result[0].toString();

            if(!resultSTR.equals(SUCCESS))
                throw new StorageException(resultSTR);
        }
        catch(StorageException e){
            throw e;
        }
        catch(Throwable e){
            throw new StorageException(e);
        }
    }
    
    /**
     * Recupera um objeto do cache.
     * 
     * @param key Identificação do objeto no cache.
     * @return Objeto ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * objeto do cache.
     */
    public synchronized Object get(String key) throws RecoverException{
        try{
            this.writer.sendMessage(GET);
            this.writer.sendMessage(key);
            this.writer.flush();

            ObjectInputStream stream = null;
            try{
                stream = new ObjectInputStream(this.reader.getStream());
                return stream.readObject();
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
                if(stream != null){
                    try{
                        stream.close();
                    }
                    catch(Exception e){}
                }
            }
        }
        catch(RecoverException e){
            throw e;
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
    }

    /**
     * Remove um objeto do cache.
     * 
     * @param key Identificação do item no cache.
     * @return Verdadeiro se o item for removido. Caso contrário falso.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
     */
    public synchronized boolean remove(String key) throws RecoverException{
        
        try{
            this.writer.sendMessage(REMOVE);
            this.writer.sendMessage(key);
            this.writer.flush();

            StringBuilder[] response = this.reader.getParameters(1);

            if(!SUCCESS.equals(response.toString()))
                throw new RecoverException(response.toString());

            return true;
        }
        catch(RecoverException e){
            throw e;
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
    }
    
    /**
     * Verifica se ainda está conectado.
     * @return Verdadeiro se ainda conectado. Caso contrário falso.
     */
    public boolean isConnected(){
        return this.socket.getInetAddress().is
    }
    /**
     * Obtém o endereço do servidor.
     * @return Endereço do servidor.
     */
    public String getHost() {
        return host;
    }

    /**
     * Obtém a porta do servidor.
     * @return Porta do servidor.
     */
    public int getPort() {
        return port;
    }
}
