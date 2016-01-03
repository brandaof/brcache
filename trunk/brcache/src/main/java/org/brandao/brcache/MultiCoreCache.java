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

import java.io.InputStream;

/**
 * Representa um cache.
 * 
 * @author Brandao
 */
public class MultiCoreCache 
    extends Cache{

    private static final long KB = 1024;

    private static final long MB = 1024*1024;

    private static final long GB = 1024*1024*1024;

    private static final long TB = 1024*1024*1024*1024;
	
    private Cache[] cores;
    
    public MultiCoreCache(
        long nodesSize,
        long nodesSwapSize,
        double nodesSwapFactor,
        long indexSize,
        long indexSwapSize,
        double indexSwapFactor,
        long dataSize,
        long dataSwapSize,
        double dataSwapFactor,
        int maxSlabSize,
        int writeBufferSize,
        int maxSizeEntry,
        int maxSizeKey,
        String dataPath,
        SwaperStrategy swaperType,
        double lockFactor,
        int quantitySwaperThread){
        
    	long cores;
    	
    	if(dataSize < 512L*MB)
    		cores = 1;
    	else
    	if(dataSize < 1L*GB)
    		cores = 2;
    	else
    	if(dataSize < 3L*GB)
    		cores = 4;
    	else
    		cores = 10;
    	
        nodesSize = (long)(nodesSize / cores);
        indexSize = (long)(indexSize / cores);
        dataSize  = (long)(dataSize / cores);
        
        this.cores = new Cache[(int)cores];
        
        for(int i=0;i<cores;i++){
            this.cores[i] = new Cache(
                nodesSize,
                nodesSwapSize,
                nodesSwapFactor,
                indexSize,
                indexSwapSize,
                indexSwapFactor,
                dataSize,
                dataSwapSize,
                dataSwapFactor,
                maxSlabSize,
                writeBufferSize,
                maxSizeEntry,
                maxSizeKey,
                dataPath + "/core" + (i+1),
                swaperType,
                lockFactor,
                quantitySwaperThread);        
            }
    }
    
    /**
     * Cria um novo cache.
     * 
     */
    public MultiCoreCache(){
        this(
        29360128L,   //28kb
        16384L,      //16kb
        0.3,
        29360128L,   //28kb
        16384L,      // 16kb
        0.3,
        536870912L,  //512mb
        1048576L,    //1mb
        0.6,
        16*1024,
        8012,
        1048576,     //1mb
        128,
        "/mnt/brcache",
        SwaperStrategy.FILE_TREE,
        0.1,
        1);
    }
    
    public void putObject(String key, long maxAliveTime, Object item) throws StorageException{
        this.cores[Math.abs(key.hashCode()) % this.cores.length].putObject(key, maxAliveTime, item);
    }

    public Object getObject(String key) throws RecoverException{
        return this.cores[Math.abs(key.hashCode()) % this.cores.length].getObject(key);
    }

    public void put(String key, long maxAliveTime, InputStream inputData) throws StorageException{
        this.cores[Math.abs(key.hashCode()) % this.cores.length].put(key, maxAliveTime, inputData);
    }
    
    public InputStream get(String key) throws RecoverException{
        return this.cores[Math.abs(key.hashCode()) % this.cores.length].get(key);
    }
    
    public boolean remove(String key) throws RecoverException{
        return this.cores[Math.abs(key.hashCode()) % this.cores.length].remove(key);
    }    
    
    public long getCountRead(){
        long value = 0;
        for (Cache core : this.cores) {
            value += core.getCountRead();
        }
        return value;
    }

    public long getCountWrite(){
        long value = 0;
        for (Cache core : this.cores) {
            value += core.getCountWrite();
        }
        return value;
    }

    public long getCountRemoved() {
        long value = 0;
        for (Cache core : this.cores) {
            value += core.getCountRemoved();
        }
        return value;
    }
    
    public long getCountReadData() {
        long value = 0;
        for (Cache core : this.cores) {
            value += core.getCountReadData();
        }
        return value;
    }
    
    public long getCountWriteData() {
        long value = 0;
        for (Cache core : this.cores) {
            value += core.getCountWriteData();
        }
        return value;
    }
    
    public long getCountRemovedData() {
        long value = 0;
        for (Cache core : this.cores) {
            value += core.getCountRemovedData();
        }
        return value;
    }
    
}
