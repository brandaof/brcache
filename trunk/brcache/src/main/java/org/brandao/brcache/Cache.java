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

import org.brandao.brcache.HugeListCalculator.HugeListInfo;
import org.brandao.brcache.collections.FileSwaper;
import org.brandao.brcache.collections.HugeArrayList;
//import org.brandao.brcache.collections.StringTreeKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.CRC32;
import org.brandao.brcache.collections.Collections;
import org.brandao.brcache.collections.DiskSwapper;
import org.brandao.brcache.collections.StringTreeMap;
import org.brandao.brcache.collections.Swapper;

/**
 * Representa um cache.
 * 
 * @author Brandao
 */
public class Cache implements Serializable{
    
    private static final long serialVersionUID = 8023029671447700902L;

    private static final int ENTRY_BINARY_SIZE = 48;
    
    private static final int NODE_BINARY_SIZE = 528 + ENTRY_BINARY_SIZE;

    private static final int INDEX_BINARY_SIZE = 40 + ENTRY_BINARY_SIZE;
    
    private final StringTreeMap<DataMap> dataMap;

    private final HugeArrayList<ByteArrayWrapper> dataList;
    
    private final int segmentSize;
    
    private final BlockingQueue<Integer> freeSegments;

    private final int maxBytesToStorageEntry;
    
    private final int maxLengthKey;
    
    private volatile long modCount;
    
    volatile long countRead;
    
    volatile long countWrite;
    
    volatile long countRemoved;
    
    volatile long countReadData;

    volatile long countWriteData;

    volatile long countRemovedData;
    
    private String dataPath;
    
    /**
     * Cria um novo cache.
     * 
     */
    public Cache(){
        this(
        29360128L,   //28kb
        16384L,      //16kb
        0.3,
        29360128L,   //28kb
        16384L,      // 16kb
        0.3,
        536870912L,  //512mb
        1048576L,    //1mb
        0.3,
        16*1024,
        1048576,     //1mb
        128,
        "/mnt/brcache",
        SwaperStrategy.FILE_TREE,
        0.1,
        16);
    }
    
    /**
     * Cria um novo cache.
     * 
     * @param nodeBufferSize Tamanho do buffer onde os nós ficarão armazenados. 
     * @param nodeSlabSize 
     * @param nodeSwapFactor
     * @param indexBufferSize
     * @param indexSlabSize
     * @param indexSwapFactor
     * @param dataBufferSize
     * @param dataSlabSize
     * @param blockSize
     * @param dataSwapFactor
     * @param maxSizeEntry
     * @param maxSizeKey
     * @param dataPath
     * @param swaperType
     * @param quantitySwaperThread
     */
    public Cache(
    		long nodeBufferSize,
    		long nodeSlabSize,
    		double nodeSwapFactor,
    		
    		long indexBufferSize,
    		long indexSlabSize,
    		double indexSwapFactor,
    		
    		long dataBufferSize,
    		long dataSlabSize,
    		long blockSize,
    		double dataSwapFactor,
    		
    		int maxSizeEntry,
    		int maxSizeKey,
            String dataPath,
            SwaperStrategy swaperType,
            int quantitySwaperThread
    		){

        synchronized(Collections.class){
	    	try{
		    	HugeListInfo dataInfo = 
		    			HugeListCalculator
		    				.calculate(dataBufferSize, dataSlabSize, blockSize, dataSwapFactor);
		        this.dataList =
		                new HugeArrayList<ByteArrayWrapper>(
		                "data",
		                dataInfo.getMaxCapacityElements(),
		                dataInfo.getClearFactorElements(),
		                dataInfo.getFragmentFactorElements(),
		                this.getSwaper(swaperType),
		                quantitySwaperThread
		                );
	    	}
	    	catch(IllegalArgumentException e){
	    		throw new IllegalArgumentException("fail create buffer", e);
	    	}
	    	
	    	try{
		    	HugeListInfo nodeInfo = 
		    			HugeListCalculator
		    				.calculate(
		    						nodeBufferSize, nodeSlabSize, 
		    						NODE_BINARY_SIZE, nodeSwapFactor);

		    	HugeListInfo indexInfo = 
		    			HugeListCalculator
		    				.calculate(indexBufferSize, indexSlabSize, 
		    						INDEX_BINARY_SIZE + (maxSizeEntry/blockSize)*4, indexSwapFactor);
		    	
	            this.dataMap =
	                    new StringTreeMap<DataMap>(
	                    "dataMap",
	                    nodeInfo.getMaxCapacityElements(),
	                    nodeInfo.getClearFactorElements(),
	                    nodeInfo.getFragmentFactorElements(),
	                    this.getSwaper(swaperType),
	                    quantitySwaperThread,
	                    indexInfo.getMaxCapacityElements(),
	                    indexInfo.getClearFactorElements(),
	                    indexInfo.getFragmentFactorElements(),
	                    this.getSwaper(swaperType),
	                    quantitySwaperThread
	                    );
	    	}
	    	catch(IllegalArgumentException e){
	    		throw new IllegalArgumentException("fail create buffer", e);
	    	}
	    	
        }
    	
    	
        this.modCount               = 0;
        this.dataPath               = dataPath;
        this.freeSegments           = new LinkedBlockingQueue<Integer>();
        this.segmentSize            = (int)blockSize;
        this.maxBytesToStorageEntry = maxSizeEntry;
        this.maxLengthKey           = maxSizeKey;
    }
    
    /**
     * Obtém a estratégia de swap dos dados do cache.
     * 
     * @param strategy Tipo da estratégia.
     * @return Estratégia.
     */
    protected Swapper getSwaper(SwaperStrategy strategy){
        Swapper swapper = new FileSwaper();
        if(swapper instanceof DiskSwapper)
            ((DiskSwapper)swapper).setRootPath(this.dataPath);
        
        return swapper;
    }
    
    /**
     * Inclui ou sobrescreve um objeto no cache.
     * 
     * @param key Identificação do objeto no cache.
     * @param maxAliveTime Tempo máximo em milesegundos que o objeto ficará no cache.
     * @param item Objeto a ser incluído no cache.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * objeto no cache.
     */
    public void putObject(String key, long maxAliveTime, Object item) throws StorageException{
        try{
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream oout = new ObjectOutputStream(bout);
            oout.writeObject(item);
            oout.flush();
            this.put(key, maxAliveTime, new ByteArrayInputStream(bout.toByteArray()));
        }
        catch(StorageException e){
            throw e;
        }
        catch(Throwable e){
            throw new StorageException(e);
        }
        
    }

    /**
     * Recupera um objeto do cache.
     * 
     * @param key Identificação do objeto no cache.
     * @return Objeto ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * objeto do cache.
     */
    public Object getObject(String key) throws RecoverException{
        try{
            InputStream in = this.get(key);
            if(in != null){
                ObjectInputStream oin = new ObjectInputStream(in);
                return oin.readObject();
            }
            else
                return null;
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
        
    }

    /**
     * Inclui ou sobrescreve um item no cache.
     * 
     * @param key Identificação do item no cache.
     * @param maxAliveTime Tempo máximo em milesegundos que o item ficará no cache.
     * @param inputData Fluxo de dados que representa o item.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
     */
    public void put(String key, long maxAliveTime, InputStream inputData) throws StorageException{
        
        if(key.length() > this.maxLengthKey)
            throw new StorageException("key is very large");
        
        //TreeKey treeKey = new StringTreeKey(key);
        //DataMap oldMap  = this.dataMap.get(key);
        DataMap oldMap  = null;
        DataMap map     = new DataMap();
        try{
            map.setId(this.modCount++);
            map.setMaxLiveTime(maxAliveTime);
            this.putData(map, inputData);
            oldMap = this.dataMap.put(key, map);
            this.countWrite++;
        }
        catch(Throwable e){
            int[] segments = map.getSegments();
        	
            if(segments != null){
                for(int segment: segments){
                    ByteArrayWrapper dataWrapper = this.dataList.get(segment);
                    this.countRemovedData += dataWrapper.buffer.length();
                    this.freeSegments.add(segment);
                }
            }
            throw 
            	e instanceof StorageException? 
            		(StorageException)e : 
            		new StorageException(e);
        }
        
    	if(oldMap != null){
            int[] segments = oldMap.getSegments();
        	
            if(segments != null){
                for(int segment: segments){
                    synchronized(this.dataList){
                        ByteArrayWrapper dataWrapper = this.dataList.get(segment);
                        if(dataWrapper != null && dataWrapper.id == oldMap.getId()){
                            this.dataList.set(segment, null);
                            this.countRemovedData += dataWrapper.buffer.length();
                            this.freeSegments.add(segment);
                        }
                    }
                }
            }
            
            this.countRemoved++;
    	}
    }

    /**
     * Recupera um item do cache.
     * 
     * @param key Identificação do item no cache.
     * @return Fluxo de dados que representa o item ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * item do cache.
     */
    public InputStream get(String key) throws RecoverException{
        
        try{
            countRead++;

            //DataMap map = this.dataMap.get(new StringTreeKey(key));
            DataMap map = this.dataMap.get(key);
            
            if(map != null){
                int[] segmentIds = map.getSegments();
                ByteArrayWrapper[] segments = new ByteArrayWrapper[segmentIds.length];
                //CRC32 crc = new CRC32();
                
                for(int i=0;i<segmentIds.length;i++){
                    ByteArrayWrapper dataWrapper = this.dataList.get(segmentIds[i]);

                    /*
                        Se dataWrapper for igual a null ou sua id for diferente da
                        id do DataMap, significa que essa entrada foi ou está sendo
                        removida.
                    */
                    if(dataWrapper == null)
                        throw new CorruptedDataException("corrupted data");

                    if(dataWrapper.id != map.getId() || dataWrapper.segment != i)
                        throw new CorruptedDataException("invalid segment: " + dataWrapper.id + ":" + map.getId() + " " + dataWrapper.segment + ":" + i);

                    segments[i] = dataWrapper;
                    //crc.update(dataWrapper.buffer.segments[0]);
                }
                
                //if(crc.getValue() != map.getCrc())
                //    throw new CorruptedDataException("bad crc: " + map.getCrc() + ":" + crc.getValue());
                
                return new CacheInputStream(this, map, segments);
                //return new CacheInputStream(this, map, this.dataList);
            }
            else
                return null;
        }
        catch(CorruptedDataException e){
            e.printStackTrace();
            return null;
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
    }
    
    /**
     * Remove um item do cache.
     * 
     * @param key Identificação do item no cache.
     * @return Verdadeiro se o item for removido. Caso contrário falso.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
     */
    public boolean remove(String key) throws RecoverException{
        
        try{
            //DataMap data = this.dataMap.get(new StringTreeKey(key));
        	DataMap data = this.dataMap.get(key);

            if(data != null){

                //this.dataMap.put(new StringTreeKey(key), null);
                this.dataMap.put(key, null);                

                    int[] segments = data.getSegments();
                    for(int segment: segments){
                        synchronized(this.dataList){
                            ByteArrayWrapper dataWrapper = this.dataList.get(segment);
                            if(dataWrapper != null && dataWrapper.id == data.getId()){
                                this.countRemovedData += dataWrapper.buffer.length();
                                this.dataList.set(segment, null);
                                this.freeSegments.put(segment);
                            }
                        }
                }
                
                countRemoved++;
                return true;
            }
            else
                return false;
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
        
    }
    
    private void putData(DataMap map, InputStream inputData) throws StorageException{
        
        int writeData = 0;
        List<Integer> segments = new ArrayList<Integer>(5);
        RegionMemory buffer = null;
        try{
            //CRC32 crc = new CRC32();
            
            int index = 0;
            buffer    = Memory.alloc(this.segmentSize);
            int read;
            
            while((read = buffer.read(inputData, 0, buffer.length())) != -1){

            	RegionMemory data = Memory.alloc(read);
            	data.write(0, buffer, 0, read);
               //crc.update(data.segments[0], 0, read);
               writeData += read;
               
               if(writeData > this.maxBytesToStorageEntry)
                   throw new StorageException("data is very large");
               
                synchronized(this.dataList){
                    Integer segment = this.freeSegments.poll();
                    if(segment == null){
                        segment = this.dataList.size();
                        this.dataList.add(new ByteArrayWrapper(map.getId(), index++, data, read));
                    }
                    else
                        this.dataList.set(segment, new ByteArrayWrapper(map.getId(), index++, data, read));
                    segments.add(segment);
                }
               
            }
            
            this.countWriteData += writeData;
            
            Integer[] segs = segments.toArray(new Integer[0]);
            int[] result = new int[segs.length];

            for(int i=0;i<segs.length;i++)
                result[i] = segs[i];
            
            map.setLength(writeData);
            map.setSegments(result);
            //map.setCrc(crc.getValue());
        }
        catch(StorageException e){
            this.countRemovedData += writeData;
            for(int segment: segments)
                this.freeSegments.add(segment);
            
            throw e;
        }
        catch(IOException e){
            this.countRemovedData += writeData;
            for(int segment: segments)
                this.freeSegments.add(segment);
            throw new StorageException(e);
        }
        finally{
        	if(buffer != null){
        		Memory.release(buffer);
        	}
        }
    }
    
    /**
     * Obtém a quantidade de item recuperados.
     * 
     * @return Quantidade de item recuperados.
     */
    public long getCountRead(){
        return this.countRead;
    }

    /**
     * Obtém a quantidade de item armazenados.
     * 
     * @return Quantidade de item armazenados.
     */
    public long getCountWrite(){
        return this.countWrite;
    }

    /**
     * Obtém a quantidade de item removidos.
     * 
     * @return Quantidade de item removidos.
     */
    public long getCountRemoved() {
		return countRemoved;
	}

    /**
     * Obtém a quantidade de bytes recuperados.
     * 
     * @return Quantidade de bytes recuperados.
     */
    public long getCountReadData() {
        return countReadData;
    }
    
    /**
     * Obtém a quantidade de bytes armazenados.
     * 
     * @return Quantidade de bytes armazenados.
     */
    public long getCountWriteData() {
        return countWriteData;
    }

    /**
     * Obtém a quantidade de bytes removidos.
     * 
     * @return Quantidade de bytes removidos.
     */
    public long getCountRemovedData() {
        return countRemovedData;
    }
    
}
