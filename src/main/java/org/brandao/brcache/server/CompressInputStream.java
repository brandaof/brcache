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
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 *
 * @author Brandao
 */
public class CompressInputStream 
    extends InflaterInputStream {

    public CompressInputStream(InputStream in, Inflater inf, int size) {
        super(in, inf, size);
    }

    public CompressInputStream(InputStream in, Inflater inf) {
        super(in, inf);
    }

    public CompressInputStream(InputStream in) {
        super(in);
    }
    
    public int read(byte[] b, int off, int len) throws IOException {
        
        //int result = super.read(b, off, len);
        int result ;
        while((result = super.read(b, off, len)) == -1){
            this.inf.reset();
        }
        return result;
        
        /*
        if(result == -1){
            if(super.len != 0){
                
               if(this.inf.finished())
                   this.inf.reset();
                   
                result = super.read(b, off, len);
            }
        }
        
        return result;
        */
    }
    
}
