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
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 *
 * @author Brandao
 */
public class CompressOutputStream 
    extends DeflaterOutputStream {

    public CompressOutputStream(OutputStream out, Deflater def) {
        super(out, def);
    }
    
    public CompressOutputStream(OutputStream out, Deflater def, int size) {
        super(out, def, size);
    }
    
    public CompressOutputStream(OutputStream out) {
        super(out);
        this.out = out;
    }
    
    @Override
    public void flush() throws IOException{
        super.finish();
        super.flush();
        def.reset();
    }
}
