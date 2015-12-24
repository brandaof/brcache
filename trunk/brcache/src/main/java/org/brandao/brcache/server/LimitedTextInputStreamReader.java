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

import java.io.EOFException;
import java.io.IOException;

/**
 *
 * @author Brandao
 */
public class LimitedTextInputStreamReader 
	extends AbstractTextInputStreamReader{

	private int size;
	
	private int read;
	
    public LimitedTextInputStreamReader(TextBufferReader buffer, int offset, int size){
    	super(buffer, offset);
    	this.size = size;
    	this.read = 0;
    }
    
    public byte[] read(int len) throws IOException{
    	
    	if(this.byteBuffer != null)
    		throw new IllegalStateException();
    	
    	byte[] result = this.buffer.readLineInBytes(len);
    	
    	if(result != null)
    		this.read += result.length;
    	
    	return result;
    }
    
	@Override
	protected byte[] readData(TextBufferReader buffer) throws IOException {
		
		if(size == read)
			return null;
		
		int toRead = size - read;
        byte[] line = buffer.readLineInBytes(toRead);
        if(line != null){
        	read += line.length;
        	return line;
        }
        else{
        	this.read = this.size;
        	return null;
        }
        
	}

	@Override
	protected boolean closeData(TextBufferReader buffer) throws IOException {
		byte[] line;
		
        if(size != read){
    		int toRead = size - read;
            line = buffer.readLineInBytes(toRead);
            read += line.length;
        }
        
        line = buffer.readLineInBytes();
        
        if(line == null || line.length != 0)
        	throw new EOFException("premature end of data");
        
        return true;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
	
}
