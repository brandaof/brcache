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
import java.util.Properties;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 *
 * @author Brandao
 */
public class CompressStreamFactory 
    implements StreamFactory{

    public CompressStreamFactory(){
        throw new UnsupportedOperationException("not implemented yet");
    }
    
    public InputStream createInpuStream(Socket socket) throws IOException {
        return new CompressInputStream(
            socket.getInputStream(), new Inflater(true), 1024);
    }

    public OutputStream createOutputStream(Socket socket) throws IOException {
        return new CompressOutputStream(
                socket.getOutputStream(),
            new Deflater(Deflater.BEST_COMPRESSION, true), 1024);
    }

    public void setConfiguration(Properties config) {
    }
    
}
