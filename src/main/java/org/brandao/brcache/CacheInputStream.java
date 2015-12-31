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

package org.brandao.brcache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Representa o fluxo de bytes de um item de um cache.
 * 
 * @author Brandao.
 */
public class CacheInputStream extends InputStream{

    private DataMap map;
    
    private List<ByteArrayWrapper> dataList;

    private ByteArrayWrapper[] arrayDataList;
    
    private int currentSegmentIndex;
    
    private int currentDataindex;
    
    private Cache cache;
    
    public CacheInputStream(Cache cache, DataMap map, List<ByteArrayWrapper> dataList){
        this.map = map;
        this.arrayDataList = null;
        this.dataList = dataList;
        this.currentDataindex = 0;
        this.currentSegmentIndex = 0;
        this.cache = cache;
    }

    public CacheInputStream(Cache cache, DataMap map, ByteArrayWrapper[] dataList){
        this.map = map;
        this.arrayDataList = dataList;
        this.dataList = null;
        this.currentDataindex = 0;
        this.currentSegmentIndex = 0;
        this.cache = cache;
    }
    
    @Override
    public int read(byte[] bytes, int i, int i1) throws IOException {
        return transfer(bytes, i, i1 );
    }
    
    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int l = transfer(b, 0, 1);
        
        if(l == -1)
            return -1;
        else
            return b[0];
    }
    
    private int transfer(byte[] dest, int destPos, int length ){
        int[] segments = this.map.getSegments();
        
        if(this.currentSegmentIndex >= segments.length)
            return -1;
        
        ByteArrayWrapper dataWrapper = this.dataList.get(segments[this.currentSegmentIndex]);
        byte[] origin  = dataWrapper.toByteArray();
        
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
                
                if(this.currentSegmentIndex < segments.length){
                	dataWrapper = this.dataList.get(segments[this.currentSegmentIndex]);
                	origin = dataWrapper == null? null : dataWrapper.toByteArray();
                }
                /*
                origin = 
                    this.currentSegmentIndex < segments.length? 
                        this.dataList.get(segments[this.currentSegmentIndex]) :
                        null;
                        */
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
    	if(this.arrayDataList == null){
	        int[] segments = this.map.getSegments();
	        
	        for(int i=0;i<segments.length;i++){
	        	ByteArrayWrapper dataWrapper = this.dataList.get(segments[i]);
	        	dataWrapper.writeTo(out);
	        }
    	}
    	else{
	        for(ByteArrayWrapper dataWrapper: this.arrayDataList){
	        	dataWrapper.writeTo(out);
	        }
    	}
        
    }
    
    public long getSize(){
    	return this.map.getLength();
    }
    
}
