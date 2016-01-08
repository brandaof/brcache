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

import org.brandao.brcache.collections.FileSwaper;
import org.brandao.brcache.collections.HugeArrayList;
import org.brandao.brcache.collections.StringTreeKey;
import org.brandao.brcache.collections.TreeHugeMap;
import org.brandao.brcache.collections.TreeKey;
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
import org.brandao.brcache.collections.Swapper;

/**
 * Representa um cache.
 * 
 * @author Brandao
 */
public class Cache implements Serializable{
    
    private final TreeHugeMap<TreeKey,DataMap> dataMap;

    private final HugeArrayList<ByteArrayWrapper> dataList;
    
    private final int segmentSize;
    
    private final BlockingQueue<Integer> freeSegments;

    private final int writeBufferLength;

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
     * @param nodesSize Quantidade de bytes usados para armazenar os nós na memória.
     * @param nodesSwapSize Tamanho do bloco de swap dos nós.
     * @param nodesSwapFactor Fator de swap dos nós.
     * @param indexSize Quantidade de bytes usados para armazenar os índices dos itens na memória.
     * @param indexSwapSize Tamanho do bloco de swap dos índices.
     * @param indexSwapFactor Fator de swap dos índices.
     * @param dataSize Quantidade de bytes usados para armazenar os itens na memória.
     * @param dataSwapSize Tamanho do bloco de swap dos itens.
     * @param dataSwapFactor Fator de swap dos itens.
     * @param maxSlabSize Tamanho do agrupamento dos dados do itens em bytes.
     * @param writeBufferSize Tamanho do buffer de escrita no cache.
     * @param maxSizeEntry Tamanho máximo em bytes que um item pode ter para ser armazenado no cache.
     * @param maxSizeKey Tamanho máximo em bytes que uma chave pode ter.
     * @param dataPath Pasta onde os dados do cache serão armazenados no processo de swap.
     * @param swaperType Estratégia de swap.
     * @param lockFactor Fator de lock. Determina quantos locks serão usados para bloquear os segmentos.
     * @param quantitySwaperThread Quantidade de threads que irão fazer o swap.
     */
    public Cache(
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
        
        if(nodesSwapSize > nodesSize)
            throw new RuntimeException("nodesSwap_size > nodesSize");

        if(indexSwapSize > indexSwapSize)
            throw new RuntimeException("indexSwapSize > indexSwapSize");

        if(maxSlabSize > dataSwapSize)
            throw new RuntimeException("maxSlabSize > dataSwapSize");

        if(dataSwapSize/maxSlabSize < 1.0)
            throw new RuntimeException("dataSwapSize must be greater than " + maxSlabSize);

        if(dataSwapSize > dataSize)
            throw new RuntimeException("dataSwapSize > dataSize");

        if(lockFactor < 0)
            throw new RuntimeException("quantityLock < 0.0");
        
        if(quantitySwaperThread < 1)
            throw new RuntimeException("quantitySwaperThread < 1");
            
        double nodesOnMemory          = nodesSize/8.0;
        double nodesPerSegment        = nodesSwapSize/8.0;
        double swapSegmentNodesFactor = nodesSwapFactor;
        
        double indexOnMemory          = indexSize/40.0;
        double indexPerSegment        = indexSwapSize/40.0;
        double swapSegmentIndexFactor = indexSwapFactor;
        
        double bytesOnMemory          = dataSize/maxSlabSize;
        double bytesPerSegment        = dataSwapSize/maxSlabSize;
        double swapSegmentsFactor     = dataSwapFactor;
        
        this.modCount               = 0;
        this.dataPath               = dataPath;
        this.freeSegments           = new LinkedBlockingQueue<Integer>();
        this.segmentSize            = maxSlabSize;
        this.writeBufferLength      = writeBufferSize;
        this.maxBytesToStorageEntry = maxSizeEntry;
        this.maxLengthKey           = maxSizeKey;
        
        synchronized(Collections.class){
            this.dataMap =
                    new TreeHugeMap<TreeKey, DataMap>(
                    "dataMap",
                    (int)nodesOnMemory,
                    swapSegmentNodesFactor,
                    nodesPerSegment/nodesOnMemory,
                    this.getSwaper(swaperType),
                    (int)((nodesOnMemory/nodesPerSegment)*lockFactor) + 1,
                    quantitySwaperThread,
                    (int)indexOnMemory,
                    swapSegmentIndexFactor,
                    indexPerSegment/indexOnMemory,
                    this.getSwaper(swaperType),
                    (int)((indexOnMemory/indexPerSegment)*lockFactor) + 1,
                    quantitySwaperThread
                    );

            this.dataList =
                    new HugeArrayList<ByteArrayWrapper>(
                    "dataList",
                    (int)bytesOnMemory,
                    swapSegmentsFactor,
                    bytesPerSegment/bytesOnMemory,
                    this.getSwaper(swaperType),
                    (int)((bytesOnMemory/bytesPerSegment)*lockFactor) + 1,
                    quantitySwaperThread
                    );
        }   
    }
    
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
        8012,
        1048576,     //1mb
        128,
        "/mnt/brcache",
        SwaperStrategy.FILE_TREE,
        0.1,
        4);
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
        
        TreeKey treeKey = new StringTreeKey(key);
        DataMap oldMap  = this.dataMap.get(treeKey);
        DataMap map     = new DataMap();
        try{
            map.setId(this.modCount++);
            map.setMaxLiveTime(maxAliveTime);
            this.putData(map, inputData);
            this.dataMap.put(treeKey, map);
            this.countWrite++;
        }
        catch(Throwable e){
            int[] segments = map.getSegments();
        	
            if(segments != null){
                for(int segment: segments){
                    ByteArrayWrapper dataWrapper = this.dataList.get(segment);
                    this.countRemovedData += dataWrapper.toByteArray().length;
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
                        if(dataWrapper != null && dataWrapper.getId() == oldMap.getId()){
                            this.dataList.set(segment, null);
                            this.countRemovedData += dataWrapper.toByteArray().length;
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

            DataMap map = this.dataMap.get(new StringTreeKey(key));
            
            if(map != null){
                int[] segmentIds = map.getSegments();
                ByteArrayWrapper[] segments = new ByteArrayWrapper[segmentIds.length];
                CRC32 crc = new CRC32();
                
                for(int i=0;i<segmentIds.length;i++){
                    ByteArrayWrapper dataWrapper = this.dataList.get(segmentIds[i]);

                    /*
                        Se dataWrapper for igual a null ou sua id for diferente da
                        id do DataMap, significa que essa entrada foi ou está sendo
                        removida.
                    */
                    if(dataWrapper == null)
                        throw new CorruptedDataException("corrupted data");

                    if(dataWrapper.getId() != map.getId() || dataWrapper.getSegment() != i)
                        throw new CorruptedDataException("invalid segment: " + dataWrapper.getId() + ":" + map.getId() + " " + dataWrapper.getSegment() + ":" + i);

                    segments[i] = dataWrapper;
                    crc.update(dataWrapper.toByteArray());
                }
                
                if(crc.getValue() != map.getCrc())
                        throw new CorruptedDataException("bad crc: " + map.getCrc() + ":" + crc.getValue());
                
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
            DataMap data = this.dataMap.get(new StringTreeKey(key));

            if(data != null){

                this.dataMap.put(new StringTreeKey(key), null);

                    int[] segments = data.getSegments();
                    for(int segment: segments){
                        synchronized(this.dataList){
                            ByteArrayWrapper dataWrapper = this.dataList.get(segment);
                            if(dataWrapper != null && dataWrapper.getId() == data.getId()){
                                this.countRemovedData += dataWrapper.toByteArray().length;
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
        
        try{
            CRC32 crc = new CRC32();
            
            byte[] buffer = new byte[this.segmentSize];
            int index     = 0;
            int read;
            
            while((read = inputData.read(buffer)) != -1){
                
               byte[] data = Arrays.copyOf(buffer, read);
               crc.update(data, 0, read);
               writeData += read;
               
               if(writeData > this.maxBytesToStorageEntry)
                   throw new StorageException("data is very large");
               
                synchronized(this.dataList){
                    Integer segment = this.freeSegments.poll();
                    if(segment == null){
                        segment = this.dataList.size();
                        this.dataList.add(new ByteArrayWrapper(map.getId(), index++, data));
                    }
                    else
                        this.dataList.set(segment, new ByteArrayWrapper(map.getId(), index++, data));
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
            map.setCrc(crc.getValue());
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
