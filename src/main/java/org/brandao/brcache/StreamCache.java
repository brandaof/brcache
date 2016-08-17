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
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.brandao.brcache.HugeListCalculator.HugeListInfo;
import org.brandao.brcache.collections.Collections;
import org.brandao.brcache.collections.DiskSwapper;
import org.brandao.brcache.collections.FileSwaper;
import org.brandao.brcache.collections.HugeArrayList;
import org.brandao.brcache.collections.StringTreeMap;
import org.brandao.brcache.collections.Swapper;
import org.brandao.brcache.collections.treehugemap.CharNode;

/**
 * Representa um cache.
 * 
 * @author Brandao
 */
public class StreamCache 
	implements Serializable{
    
    private static final long serialVersionUID = 8023029671447700902L;

    private static final int ENTRY_BINARY_SIZE = 48;
    
    private static final int NODE_BINARY_SIZE = /*528*/ CharNode.DATA_SIZE + ENTRY_BINARY_SIZE;

    private static final int INDEX_BINARY_SIZE = 34 + ENTRY_BINARY_SIZE;
    
    private final StringTreeMap<DataMap> dataMap;

    private final HugeArrayList<Block> dataList;
    
    private final int segmentSize;
    
    private final BlockingQueue<Integer> freeSegments;

    private final long maxBytesToStorageEntry;
    
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
    public StreamCache(){
        this(
    		3L*1024L*1024L, 1024, 0.5, 
    		1L*1024L*1024L, 1024, 0.5, 
    		10L*1024L*1024L, 64*1024, 1*1024, 0.5, 
    		1*1024*1024L, 100, "/mnt/brcache", SwaperStrategy.FILE, 1);
    }
    
    /**
     * Cria um novo cache.
     * 
     * @param nodeBufferSize Tamanho do buffer, em bytes, onde os nós ficarão armazenados. 
     * @param nodePageSize Tamanho da página, em bytes, do buffer de nós.
     * @param nodeSwapFactor Fator de permuta dos nós.
     * @param indexBufferSize Tamanho do buffer, em bytes, onde os índices ficarão armazenados.
     * @param indexPageSize Tamanho da página, em bytes, do buffer de índices.
     * @param indexSwapFactor Fator de permuta dos índices.
     * @param dataBufferSize Tamanho do buffer, em bytes, onde os dados ficarão armazenados. 
     * @param dataPageSize Tamanho da página, em bytes, do buffer de dados.
     * @param blockSize Tamanho do bloco, em bytes.
     * @param dataSwapFactor Fator de permuta dos dados.
     * @param maxSizeEntry Tamanho máximo de uma entrada no cache.
     * @param maxSizeKey Tamanho máximo de uma chave.
     * @param dataPath Pasta onde os dados do cache serão persistidos.
     * @param swaperType Estratégia de armazenamento dos dados em disco.
     * @param quantitySwaperThread Quantidade de processos usados para fazer a permuta.
     */
    public StreamCache(
    		long nodeBufferSize,
    		long nodePageSize,
    		double nodeSwapFactor,
    		
    		long indexBufferSize,
    		long indexPageSize,
    		double indexSwapFactor,
    		
    		long dataBufferSize,
    		long dataPageSize,
    		long blockSize,
    		double dataSwapFactor,
    		
    		long maxSizeEntry,
    		int maxSizeKey,
            String dataPath,
            SwaperStrategy swaperType,
            int quantitySwaperThread
    		){

        synchronized(Collections.class){
        	Collections.setPath(dataPath);
	    	HugeListInfo nodeInfo;
	    	HugeListInfo indexInfo;
	    	HugeListInfo dataInfo; 
        	
	    	try{
		    	dataInfo = 
		    			HugeListCalculator
		    				.calculate(dataBufferSize, dataPageSize, blockSize, dataSwapFactor);
		        this.dataList =
		                new HugeArrayList<Block>(
		                "data",
		                dataInfo.getMaxCapacityElements(),
		                dataInfo.getClearFactorElements(),
		                dataInfo.getFragmentFactorElements(),
		                this.getSwaper(swaperType),
		                quantitySwaperThread
		                );
	    	}
	    	catch(IllegalArgumentException e){
	    		throw new IllegalArgumentException("fail create data buffer", e);
	    	}

	    	
	    	try{
		    	nodeInfo = 
		    			HugeListCalculator
		    				.calculate(
		    						nodeBufferSize, nodePageSize, 
		    						NODE_BINARY_SIZE, nodeSwapFactor);
	    	}
	    	catch(IllegalArgumentException e){
	    		throw new IllegalArgumentException("fail create nodes buffer", e);
	    	}
	    	
	    	try{
		    	indexInfo = 
		    			HugeListCalculator
		    				.calculate(indexBufferSize, indexPageSize, 
		    						INDEX_BINARY_SIZE, indexSwapFactor);
	    	}
	    	catch(IllegalArgumentException e){
	    		throw new IllegalArgumentException("fail create index buffer", e);
	    	}
		    	
	    	try{
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
	    		throw new IllegalArgumentException("fail data map", e);
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
     * Inclui ou sobrescreve um item no cache.
     * @param key Identificação do item no cache.
     * @param maxAliveTime Tempo máximo em milesegundos que o item ficará no cache.
     * @param inputData Fluxo de dados que representa o item.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
     */
    public void put(String key, long maxAliveTime, InputStream inputData) throws StorageException{
        
        if(key.length() > this.maxLengthKey)
            throw new StorageException("key is very large");
        
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
    		this.releaseSegments(map);
            throw 
            	e instanceof StorageException? 
            		(StorageException)e : 
            		new StorageException(e);
        }
        finally{
	    	if(oldMap != null){
	    		this.releaseSegments(oldMap);
	            this.countRemoved++;
	    	}
        }
    }

    /**
     * Recupera um item do cache.
     * @param key Identificação do item no cache.
     * @return Fluxo de dados que representa o item ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * item do cache.
     */
    public InputStream getStream(String key) throws RecoverException{
        
        try{
            countRead++;

            DataMap map = this.dataMap.get(key);
            
            if(map != null){
                Block[] segments = new Block[map.getSegments()];
                Block current = this.dataList.get(map.getFirstSegment());
                int i=0;
                while(current != null){

                    /*
                    Se id for diferente da
                    id do DataMap, significa que essa entrada foi ou está sendo
                    removida.
                    */
					if(current.id != map.getId() || current.segment != i)
					    throw new CorruptedDataException("invalid segment: " + current.id + ":" + map.getId() + " " + current.segment + ":" + i);
                    
                    segments[i] = current;
                	current = current.nextBlock < 0? null : this.dataList.get(current.nextBlock);
                	i++;
                }
                
                return new CacheInputStream(this, map, segments);
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
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
     */
    public boolean remove(String key) throws StorageException{
        
        try{
        	DataMap data = this.dataMap.get(key);

            if(data != null){
            	this.dataMap.put(key, null);                
            	this.releaseSegments(data);
                countRemoved++;
                return true;
            }
            else
                return false;
        }
        catch(Throwable e){
            throw new StorageException(e);
        }
        
    }
    
    private void putData(DataMap map, InputStream inputData) throws StorageException{
        
        int writeData = 0;
        RegionMemory buffer = null;
        try{
            int index = 0;
            buffer    = Memory.alloc(this.segmentSize);
            int read;
            Block lastBlock = null;
            int lastSegment = -1;
            
            while((read = buffer.read(inputData, 0, buffer.length())) != -1){

            	RegionMemory data = Memory.alloc(read);
            	if(read < this.segmentSize){
            		data = Memory.alloc(read);
                	data.write(0, buffer, 0, read);
            	}
            	else
            		data = buffer;
            	
            	buffer = Memory.alloc(this.segmentSize);
            	
            	writeData += read;
            	
        		if(writeData > this.maxBytesToStorageEntry)
        			throw new StorageException("data is very large");
               
                synchronized(this.dataList){
                	Block block = new Block(map.getId(), index++, data, read);
                    Integer segment = this.freeSegments.poll();
                    if(segment == null){
                        segment = this.dataList.size();
                        this.dataList.add(block);
                    }
                    else
                        this.dataList.set(segment, block);
                    
                    if(lastBlock != null){
                    	lastBlock.nextBlock = segment;
                    	this.dataList.set(lastSegment, lastBlock);
                    }
                    else
                    	map.setFirstSegment(segment);
                    
                	lastBlock   = block;
                    lastSegment = segment;
                }
               
            }
            
            this.countWriteData += writeData;
            
            map.setLength(writeData);
            map.setSegments(index);
        }
        catch(StorageException e){
            this.countRemovedData += writeData;
            this.releaseSegments(map);
            throw e;
        }
        catch(IOException e){
            this.countRemovedData += writeData;
            this.releaseSegments(map);
            throw new StorageException(e);
        }
        /*finally{
        	if(buffer != null){
        		Memory.release(buffer);
        	}
        }*/
    }
    
    private void releaseSegments(DataMap map){
    	int segmentId = map.getFirstSegment();
    	
    	if(segmentId == -1)
    		return;
    	
    	synchronized(this.dataList){
	        Block current = this.dataList.get(segmentId);
	        
	        int i=0;
	        while(current != null){
				if(current.id == map.getId() && current.segment == i){
					this.freeSegments.add(segmentId);
				}
	            
				segmentId = current.nextBlock;
	        	current = segmentId < 0? null : this.dataList.get(segmentId);
	        	i++;
	        }
    	}
    	
    	map.setFirstSegment(-1);
    }
    
    /**
     * Obtém a quantidade de itens recuperados.
     * 
     * @return Quantidade de itens recuperados.
     */
    public long getCountRead(){
        return this.countRead;
    }

    /**
     * Obtém a quantidade de itens armazenados.
     * 
     * @return Quantidade de itens armazenados.
     */
    public long getCountWrite(){
        return this.countWrite;
    }

    /**
     * Obtém a quantidade de itens removidos.
     * 
     * @return Quantidade de itens removidos.
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
