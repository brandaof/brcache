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

/**
 *
 * @author Brandao
 */
class TextOutputStream extends OutputStream{

    private final TextBufferWriter buffer;
    
    private boolean closed;
    
    public TextOutputStream(TextBufferWriter buffer){
        this.buffer = buffer;
        this.closed = false;
    }
    
    @Override
    public void write(int i) throws IOException {
        
        if(closed)
            throw new IOException(TerminalConstants.STREAM_CLOSED);
        
        this.buffer.write(i);
    }
 
    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        
        if(closed)
            throw new IOException(TerminalConstants.STREAM_CLOSED);
        
        this.buffer.write(bytes, i, i1);
    }
    
    @Override
    public void flush() throws IOException{
        
        if(closed)
            throw new IOException(TerminalConstants.STREAM_CLOSED);
        
        this.buffer.flush();
    }
    
    @Override
    public void close() throws IOException{
        
        if(closed)
            throw new IOException(TerminalConstants.STREAM_CLOSED);
        
        this.closed = true;
    }
}
