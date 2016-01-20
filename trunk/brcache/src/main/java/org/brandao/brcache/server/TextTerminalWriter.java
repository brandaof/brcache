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
import java.io.OutputStream;
import java.net.Socket;

/**
 *
 * @author Brandao
 */
public class TextTerminalWriter implements TerminalWriter{

    private TextBufferWriter buffer;
    
    private int writeBufferSize;
    
    public TextTerminalWriter(Socket socket, StreamFactory streamFactory, 
            int writeBufferSize) throws IOException{
        this.writeBufferSize = writeBufferSize;
        this.buffer = new TextBufferWriter(
                this.writeBufferSize, 
                streamFactory.createOutputStream(socket));
    }

    public void sendMessage(String message) throws WriteDataException {
        try{
            this.buffer.write(message.getBytes());
            this.buffer.write(TerminalConstants.CRLF);
        }
        catch(IOException e){
            throw new WriteDataException(TerminalConstants.SEND_MESSAGE_FAIL, e);
        }
    }

    public void sendCRLF() throws WriteDataException {
        try{
            this.buffer.write(TerminalConstants.CRLF);
        }
        catch(IOException e){
            throw new WriteDataException(TerminalConstants.SEND_MESSAGE_FAIL, e);
        }
            
    }

    public void flush() throws WriteDataException {
        try{
            this.buffer.flush();
        }
        catch(IOException e){
            throw new WriteDataException(TerminalConstants.SEND_MESSAGE_FAIL, e);
        }
    }
    
    public OutputStream getStream() {
        return new TextOutputStream(this.buffer);
    }

	public OutputStream getDirectStream() {
		return this.buffer.getDirectOutputStream();
	}
    
}
