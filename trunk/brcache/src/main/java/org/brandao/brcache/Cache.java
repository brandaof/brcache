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

import com.brandao.uoutec.commons.collections.HugeArrayList;
import com.brandao.uoutec.commons.collections.StringTreeKey;
import com.brandao.uoutec.commons.collections.TreeHugeMap;
import com.brandao.uoutec.commons.collections.TreeKey;
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

/**
 * Representa um cache.
 * 
 * @author Brandao
 */
public class Cache implements Serializable{
    
    private final TreeHugeMap<TreeKey,DataMap> dataMap;

    private final HugeArrayList<byte[]> dataList;
    
    private final int segmentSize;
    
    private final BlockingQueue<Integer> freeSegments;

    private final int writeBufferLength;

    private final int maxBytesToStorageEntry;
    
    private final int maxLengthKey;
    
    volatile long countRead;
    
    volatile long countWrite;
    
    volatile long countReadData;

    volatile long countWriteData;

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
     */
    public Cache(
        int nodesSize,
        int nodesSwapSize,
        double nodesSwapFactor,
        int indexSize,
        int indexSwapSize,
        double indexSwapFactor,
        int dataSize,
        int dataSwapSize,
        double dataSwapFactor,
        int maxSlabSize,
        int writeBufferSize,
        int maxSizeEntry,
        int maxSizeKey,
        String dataPath){
        
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
        
        double nodesOnMemory          = nodesSize/8.0;
        double nodesPerSegment        = nodesSwapSize/8.0;
        double swapSegmentNodesFactor = nodesSwapFactor;
        
        double indexOnMemory          = indexSize/40.0;
        double indexPerSegment        = indexSwapSize/40.0;
        double swapSegmentIndexFactor = indexSwapFactor;
        
        double bytesOnMemory          = dataSize/maxSlabSize;
        double bytesPerSegment        = dataSwapSize/maxSlabSize;
        double swapSegmentsFactor     = dataSwapFactor;
        
        this.freeSegments           = new LinkedBlockingQueue<Integer>();
        this.segmentSize            = maxSlabSize;
        this.writeBufferLength      = writeBufferSize;
        this.maxBytesToStorageEntry = maxSizeEntry;
        this.maxLengthKey           = maxSizeKey;
        
        this.dataMap =
                new TreeHugeMap<TreeKey, DataMap>(
                dataPath,
                "data",
                (int)nodesOnMemory,
                swapSegmentNodesFactor,
                nodesPerSegment/nodesOnMemory,
                (int)indexOnMemory,
                swapSegmentIndexFactor,
                indexPerSegment/indexOnMemory
                );
        
        this.dataList =
                new HugeArrayList<byte[]>(
                dataPath,
                "data",
                (int)bytesOnMemory,
                swapSegmentsFactor,
                bytesPerSegment/bytesOnMemory
                );
        
    }
    
    public Cache(){
        throw new UnsupportedOperationException();
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
        
        TreeKey treeKey = new StringTreeKey(key);
        
        int[] segments = null;
        
        if(key.length() > this.maxLengthKey)
            throw new StorageException("key is very large");
        
        try{
            segments = this.putData(inputData);
            this.putSegments(treeKey, maxAliveTime, segments);
            
            countWrite++;
        }
        catch(StorageException e){
            if(segments != null){
                for(int segment: segments)
                    this.freeSegments.add(segment);
            }
            throw e;
        }
        catch(Throwable e){
            if(segments != null){
                for(int segment: segments)
                    this.freeSegments.add(segment);
            }
            throw new StorageException(e);
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

            if(map != null)
                return new CacheInputStream(this, map, this.dataList);
            else
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

                synchronized(this.dataList){
                    int[] segments = data.getSegments();

                    for(int segment: segments){
                        this.dataList.set(segment, null);
                            this.freeSegments.put(segment);
                    }
                }
                return true;
            }
            else
                return false;
        }
        catch(Throwable e){
            throw new RecoverException(e);
        }
        
    }
    
    private void putSegments(TreeKey key, long maxAliveTime, int[] segmens){
        DataMap map = new DataMap();
        map.setMaxLiveTime(maxAliveTime);
        map.setSegments(segmens);
        this.dataMap.put(key, map);
    }
    
    private int[] putData(InputStream inputData) throws StorageException{
        
        int writeData = 0;
        List<Integer> segments = new ArrayList<Integer>();
        
        try{
            byte[] buffer = new byte[this.writeBufferLength];
            int read;

            while((read = inputData.read(buffer)) != -1){
               this.writeOnCache(buffer, 0, read, segments);
               writeData += read;
               
               if(writeData > this.maxBytesToStorageEntry)
                   throw new StorageException("data is very large");
            }
            
            Integer[] segs = segments.toArray(new Integer[0]);
            int[] result = new int[segs.length];

            for(int i=0;i<segs.length;i++)
                result[i] = segs[i];
            
            return result;
        }
        catch(StorageException e){
            for(int segment: segments)
                this.freeSegments.add(segment);
            throw e;
        }
        catch(IOException e){
            for(int segment: segments)
                this.freeSegments.add(segment);
            throw new StorageException(e);
        }
        
    }

    private void writeOnCache(byte[] readBuf, int start, int offset, List<Integer> segments){
        
        byte[] writeBuf = new byte[this.segmentSize];
        int currentOffset = 0;
        
        int read = 0;
        int limitRead = offset - start;
        int maxWrite;
        int maxRead = 0;
        
        while(read < limitRead){
            
            maxWrite = writeBuf.length - currentOffset;
            maxRead  = limitRead - read;
            
            if(maxWrite > maxRead){
                System.arraycopy(readBuf, read, writeBuf, currentOffset, maxRead);
                currentOffset += maxRead;
                read += maxRead;
                this.countWriteData += maxRead;
            }
            else{
                System.arraycopy(readBuf, read, writeBuf, currentOffset, maxWrite);
                currentOffset += maxWrite;
                read += maxWrite;
                this.countWriteData += maxWrite;
                
                Integer segment = this.freeSegments.poll();
                synchronized(this.dataList){
                    if(segment == null){
                            segment = this.dataList.size();
                            this.dataList.add(writeBuf);
                    }
                    else
                        this.dataList.set(segment, writeBuf);
                }

                segments.add(segment);

                writeBuf = new byte[this.segmentSize];
                currentOffset = 0;
            }
        }
        
        if(currentOffset != 0){
            Integer segment = this.freeSegments.poll();
            byte[] tmp = Arrays.copyOf(writeBuf, maxRead);
            synchronized(this.dataList){
                if(segment == null){
                    segment = this.dataList.size();
                    this.dataList.add(tmp);
                }
                else
                    this.dataList.set(segment, tmp);
            }

            segments.add(segment);
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
    
}
