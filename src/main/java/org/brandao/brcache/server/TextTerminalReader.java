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
import java.net.Socket;

/**
 *
 * @author Brandao
 */
public class TextTerminalReader implements TerminalReader{

    private Socket socket;
    
    private InputStream stream;
    
    private TextBufferReader buffer;
    
    private int offset;
    
    public TextTerminalReader(Socket socket, int readBufferSize) throws IOException{
        this.socket = socket;
        this.stream = socket.getInputStream();
        this.buffer = new TextBufferReader(readBufferSize, this.stream);
        this.offset = 0;
    }
    
    public Command getCommand() throws UnknowCommandException, ReadDataException {
        StringBuilder line = null;
        try{
            line = this.buffer.readLine();
            return Command.valueOf(line.toString());
        }
        catch(IOException e){
            throw new ReadDataException(String.format(TerminalConstants.CANT_READ_COMMAND, String.valueOf(line)));
        }
        catch(NullPointerException e){
            throw new UnknowCommandException("undefined");
        }
        catch(IllegalArgumentException e){
            throw new UnknowCommandException(String.valueOf(line));
        }
    }

    public StringBuilder[] getParameters(int size) throws ReadDataException, ParameterException {
        StringBuilder[] params = new StringBuilder[size];
        
        for(int i=0;i<size;i++){
            try{
                params[i] = this.buffer.readLine();
                if(params[i] == null)
                    throw new ParameterException(String.format(TerminalConstants.EMPTY_PARAMETER, String.valueOf(i)));
            }
            catch(IOException e){
                throw new ReadDataException(String.format(TerminalConstants.CANT_READ_PARAMETER, String.valueOf(i)));
            }
        }
        
        return params;
    }

    public InputStream getStream() {
        return new TextInputStreamReader(buffer, this.offset);
    }

    public int getOffset() {
        return offset;
    }
    
}
