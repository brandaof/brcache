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

package org.brandao.brcache.ncache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.brandao.brcache.ByteArrayWrapper;
import org.brandao.brcache.CacheInputStream;
import org.brandao.brcache.DataMap;
import org.brandao.brcache.ncache.NCache;

/**
 * Representa o fluxo de bytes de um item de um cache.
 * 
 * @author Brandao.
 */
public class NCacheInputStream extends CacheInputStream{

    private DataMap map;
    
    private MemoryManager memoryManager;
    
    private int currentSegmentIndex;
    
    private int currentDataindex;
    
    private NCache cache;
    
    public NCacheInputStream(NCache cache, DataMap map, MemoryManager memoryManager){
    	super(cache, map, (List<ByteArrayWrapper>)null);
        this.map = map;
        this.memoryManager = memoryManager;
        this.currentDataindex = 0;
        this.currentSegmentIndex = 0;
        this.cache = cache;
    }
    
    @Override
    public int read(byte[] bytes, int i, int i1) throws IOException {
    	try{
    		return transfer(bytes, i, i1 );
    	}
    	catch(InterruptedException e){
    		throw new IOException(e);
    	}
    }
    
    @Override
    public int read() throws IOException {
    	try{
	        byte[] b = new byte[1];
	        int l = transfer(b, 0, 1);
	        
	        if(l == -1)
	            return -1;
	        else
	            return b[0];
    	}
    	catch(InterruptedException e){
    		throw new IOException(e);
    	}
    }
    
    private int transfer(byte[] dest, int destPos, int length ) throws InterruptedException{
        int[] segments = this.map.getSegments();
        
        if(this.currentSegmentIndex >= segments.length)
            return -1;
        
        byte[] origin  = this.memoryManager.read(segments[this.currentSegmentIndex]);
        
        int read = 0;
        
        while(length > 0 && origin != null){
            
            if(length > origin.length - this.currentDataindex){
                
                int lenRead = origin.length - this.currentDataindex;
                
                System.arraycopy(origin, this.currentDataindex, dest, destPos, lenRead);
                
                cache.countReadData += lenRead;
                length -= lenRead;
                read += lenRead;
                destPos += lenRead;
                this.currentDataindex = 0;
                this.currentSegmentIndex++;
                
                origin = 
                    this.currentSegmentIndex < segments.length? 
                    		this.memoryManager.read(segments[this.currentSegmentIndex]) :
                        null;
            }
            else{
                int lenRead = length;
                System.arraycopy(origin, this.currentDataindex, dest, destPos, lenRead);
                
                cache.countReadData += lenRead;
                destPos += lenRead;
                read += lenRead;
                length -= lenRead;
                this.currentDataindex += lenRead;
            }
        }
        
        return read;
    }
    
    public void writeTo(OutputStream out) throws IOException{
    	try{
	        int[] segments = this.map.getSegments();
	        for(int i=0;i<segments.length;i++){
	        	byte[] data = this.memoryManager.read(segments[i]);
	        	out.write(data, 0, data.length);
	        }
    	}
    	catch(Throwable e){
    		throw new IOException(e);
    	}
        
    }
    
    public long getSize(){
    	return this.map.getLength();
    }
    
}
