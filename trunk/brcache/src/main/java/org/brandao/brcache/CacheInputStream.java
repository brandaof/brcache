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

/**
 * Representa o fluxo de bytes de um item de um cache.
 * 
 * @author Brandao.
 */
public class CacheInputStream extends InputStream{

    private DataMap map;
    
    private Block[] arrayDataList;
    
    private int currentSegmentIndex;
    
    private int currentDataindex;

    private Cache cache;
    
    private byte[] bufByte = new byte[1];
    
    public CacheInputStream(Cache cache, DataMap map, Block[] dataList){
        this.map = map;
        this.arrayDataList = dataList;
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
        //byte[] b = new byte[1];
        int l = transfer(bufByte, 0, 1);
        
        if(l == -1)
            return -1;
        else
            return bufByte[0];
    }
    
    private int transfer(byte[] dest, int destPos, int length ){
        
        if(this.currentSegmentIndex >= arrayDataList.length)
            return -1;
        
        Block block = arrayDataList[this.currentSegmentIndex];
        RegionMemory origin  = block.buffer;
        
        int read = 0;
        
        while(length > 0 && origin != null){
            
            if(length > block.length - this.currentDataindex){
                
                int lenRead = block.length - this.currentDataindex;
                
                origin.read(this.currentDataindex, dest, destPos, lenRead);
                
                cache.countReadData 	+= lenRead;
                length 					-= lenRead;
                read 					+= lenRead;
                destPos 				+= lenRead;
                this.currentDataindex 	= 0;
                this.currentSegmentIndex++;
                
                if(this.currentSegmentIndex < arrayDataList.length){
                	block = arrayDataList[this.currentSegmentIndex];
                	origin = block == null? null : block.buffer;
                }
            }
            else{
                int lenRead = length;
                origin.read(this.currentDataindex, dest, destPos, lenRead);
                
                cache.countReadData 	+= lenRead;
                destPos 				+= lenRead;
                read 					+= lenRead;
                length 					-= lenRead;
                this.currentDataindex 	+= lenRead;
            }
        }
        
        return read;
    }
    
    public void writeTo(OutputStream out) throws IOException{
        for(Block block: this.arrayDataList){
        	block.buffer.write(out, 0, block.length);
        }
    }
    
    public long getSize(){
    	return this.map.getLength();
    }
    
}