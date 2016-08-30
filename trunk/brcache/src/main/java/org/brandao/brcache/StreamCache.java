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

import java.io.File;
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
 * É a base para um cache. Ele faz o mapeamento chave-fluxo de 
 * bytes. Uma chave somente pode estar associado a um 
 * fluxo de bytes. Não são permtidos chaves duplicadas. 
 * 
 * <pre>
 * ex:
 *    
 *    Cache cache = ...;
 *    InputStream valueStream = ...;
 *    cache.putStream("uma_chave", valueStream, 1200);
 *    
 * ex2:
 *    
 *    InputStream value = cache.getStream("uma_chave");
 *
 * </pre>
 * 
 * @author Brandao
 */
public abstract class StreamCache 
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
    
    private boolean deleteOnExit;
    
    /*
    public StreamCache(){
        this(
    		3L*1024L*1024L, 1024, 0.5, 
    		1L*1024L*1024L, 1024, 0.5, 
    		10L*1024L*1024L, 64*1024, 1*1024, 0.5, 
    		1*1024*1024L, 100, "/mnt/brcache", SwaperStrategy.FILE, 1);
    }
    */
    
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

        this.modCount               = 0;
        this.dataPath               = dataPath;
        this.freeSegments           = new LinkedBlockingQueue<Integer>();
        this.segmentSize            = (int)blockSize;
        this.maxBytesToStorageEntry = maxSizeEntry;
        this.maxLengthKey           = maxSizeKey;
        this.deleteOnExit           = true;
    	
        synchronized(Collections.class){
        	//Collections.setPath(dataPath);
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
		        this.dataList.setDeleteOnExit(false);
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
		        this.dataMap.setDeleteOnExit(false);
	    	}
	    	catch(IllegalArgumentException e){
	    		throw new IllegalArgumentException("fail data map", e);
	    	}
	    	
        }
    	
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
	 * Associa o fluxo de bytes do valor à chave.
	 * @param key chave associada ao fluxo.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o item for substituido. Caso contrário, <code>false</code>
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    protected boolean putStream(String key, long timeToLive, long timeToIdle, 
    		InputStream inputData) throws StorageException{
        
    	if(timeToLive < 0)
            throw new StorageException(CacheErrors.ERROR_1029);

    	if(timeToIdle < 0)
            throw new StorageException(CacheErrors.ERROR_1028);
    	
        if(key.length() > this.maxLengthKey)
            throw new StorageException(CacheErrors.ERROR_1008);
        
        DataMap oldMap  = null;
        DataMap map     = new DataMap();
        
        try{
        	//ItemCacheInputStream permite manipular além dos dados os metadados do item.
            if(inputData instanceof ItemCacheInputStream){
            	ItemCacheInputStream input = (ItemCacheInputStream)inputData;
            	DataMap itemMetadata = input.getMap();
            	
                map.setCreationTime(itemMetadata.getCreationTime());
                map.setMostRecentTime(itemMetadata.getMostRecentTime());
                map.setTimeToIdle(itemMetadata.getTimeToIdle());
                map.setTimeToLive(itemMetadata.getTimeToLive());
                
            	//o cache transacional pode tentar restaurar um item já expirado.
                //Nesse caso, tem que remove-lo. 
                //Somente será removido se o item ainda for o mesmo gerenciado pela transação.
                if(map.isDead()){
                	this.remove(key, map);
                	return false;
                }
                
            }
            else{
            	//Gera os metadados do item.
	            map.setCreationTime(System.currentTimeMillis());
	            map.setMostRecentTime(map.getCreationTime());
	            map.setTimeToIdle(timeToIdle);
	            map.setTimeToLive(timeToLive);
            }
            
            //Toda item inserido tem que ter uma nova id. Mesmo que ele exista.
            map.setId(this.modCount++);
            //Registra os dados no buffer de dados.
            this.putData(map, inputData);
            //Faz a indexação do item e retorna o índice atual, caso exista.
            oldMap = this.dataMap.put(key, map);
            
            this.countWrite++;
            return oldMap != null;
        }
        catch(Throwable e){
        	try{
        		this.releaseSegments(map);
        	}
        	catch(Throwable ex){
        		e.printStackTrace();
        	}
            throw 
            	e instanceof StorageException? 
            		(StorageException)e : 
            		new StorageException(e, CacheErrors.ERROR_1020);
        }
        finally{
	    	if(oldMap != null){
	    		this.releaseSegments(oldMap);
	            this.countRemoved++;
	    	}
        }
    }

    /**
     * Substitui o fluxo de bytes associado à chave somente se ele existir.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    protected boolean replaceStream(String key, long timeToLive, long timeToIdle, 
    		InputStream inputData) throws StorageException{
        
    	if(timeToLive < 0)
            throw new StorageException(CacheErrors.ERROR_1029);

    	if(timeToIdle < 0)
            throw new StorageException(CacheErrors.ERROR_1028);
    	
        if(key.length() > this.maxLengthKey)
            throw new StorageException(CacheErrors.ERROR_1008);
        
        DataMap oldMap = null;
        DataMap map    = new DataMap();
        
        map.setCreationTime(System.currentTimeMillis());
        map.setMostRecentTime(map.getCreationTime());
        map.setTimeToIdle(timeToIdle);
        map.setTimeToLive(timeToLive);
        
        //Toda item inserido tem que ter uma nova id. Mesmo que ele exista.
        map.setId(this.modCount++);

        try{
            //Registra os dados no buffer de dados.
            this.putData(map, inputData);
        }
        catch(Throwable e){
        	try{
        		this.releaseSegments(map);
        	}
        	catch(Throwable ex){
        		ex.printStackTrace();
        	}
        	
            throw 
        	e instanceof StorageException? 
        		(StorageException)e : 
        		new StorageException(e, CacheErrors.ERROR_1020);
        	
        }

        try{
            //Faz a indexação do item e retorna o índice atual, caso exista.
            oldMap = this.dataMap.replace(key, map);
        }
        catch(Throwable e){
        	try{
    	    	this.releaseSegments(map);
            	this.dataMap.remove(key, map);
        	}
        	catch(Throwable ex){
        		ex.printStackTrace();
        	}
        	
        	throw new StorageException(e, CacheErrors.ERROR_1020);
        }
        finally{
	    	if(oldMap != null){
	    		this.releaseSegments(oldMap);
	    	}
        }
        
        if(oldMap != null){
            this.countWrite++;
        	return true;
        }
        else
        	return false;
    }
    
    /**
     * Associa o fluxo de bytes do valor à chave somente se ele não existir.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o fluxo for associado à chave. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    protected InputStream putIfAbsentStream(String key, long timeToLive, long timeToIdle, 
    		InputStream inputData) throws StorageException{
        
    	if(timeToLive < 0)
            throw new StorageException(CacheErrors.ERROR_1029);

    	if(timeToIdle < 0)
            throw new StorageException(CacheErrors.ERROR_1028);
    	
        if(key.length() > this.maxLengthKey)
            throw new StorageException(CacheErrors.ERROR_1008);
        
        DataMap oldMap = null;
        DataMap map    = new DataMap();
        InputStream in = null;

        map.setCreationTime(System.currentTimeMillis());
        map.setMostRecentTime(map.getCreationTime());
        map.setTimeToIdle(timeToIdle);
        map.setTimeToLive(timeToLive);
        
        //Toda item inserido tem que ter uma nova id. Mesmo que ele exista.
        map.setId(this.modCount++);

        try{
            //Registra os dados no buffer de dados.
            this.putData(map, inputData);
        }
        catch(Throwable e){
        	try{
        		this.releaseSegments(map);
        	}
        	catch(Throwable ex){
        		ex.printStackTrace();
        	}
        	
            throw 
        	e instanceof StorageException? 
        		(StorageException)e : 
        		new StorageException(e, CacheErrors.ERROR_1020);
        	
        }

        try{
            //Faz a indexação do item e retorna o índice atual, caso exista.
            oldMap = this.dataMap.putIfAbsent(key, map);
        }
        catch(Throwable e){
        	try{
    	    	this.releaseSegments(map);
            	this.dataMap.remove(key, map);
        	}
        	catch(Throwable ex){
        		ex.printStackTrace();
        	}
        	
        	throw new StorageException(e, CacheErrors.ERROR_1020);
        }
        
    	//se oldMap for diferente de null, significa que já existe um item no cache
        if(oldMap != null){
        	//remove os segmentos alocados para o item atual.
        	//se oldMap for diferente de null, map não foi registrado
        	//somente precisa liberar os segmentos alocados
    		this.releaseSegments(map);
    		
        	//tenta obter o stream do item no cache
        	in = this.getStream(key, map);
        }
        
    	this.countWrite++;
        
        if(oldMap != null){
	    	if(in == null){
	    		//será lançada uma exceção se o item não existir
	    		throw new StorageException(CacheErrors.ERROR_1030);
	    	}
	    	else{
	    		//retorna o stream
	    		return in;
	    	}
        }
        else{
        	return null;
        }
        
    }
    
    /**
     * Obtém o fluxo de bytes do valor associado à chave.
     * @param key chave associada ao fluxo.
     * @return fluxo de bytes do valor ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
     */
    
    protected InputStream getStream(String key) throws RecoverException {
        DataMap map = this.dataMap.get(key);
    	return this.getStream(key, map);
    }
    
    private InputStream getStream(String key, DataMap map) throws RecoverException {
        
        try{
            countRead++;

            if(map != null){
            	
            	//Verifica se o item já expirou
            	if(map.isDead()){
            		//Se expirou, remove do cache e retorna null.
            		this.remove(key, map);
            		return null;
            	}
            	
            	//Se timeToIdle foi definido, é atualizado o horário do último acesso.
            	if(map.getTimeToIdle() > 0){
	            	map.setMostRecentTime(System.currentTimeMillis());
	            	//a instância no momento do replace porde não ser a mesma passada em oldElement.
	            	this.dataMap.replace(key, map, map);
            	}
            	
                Block[] segments = new Block[map.getSegments()];
                Block current    = this.dataList.get(map.getFirstSegment());
                int i            = 0;
                
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
            throw new RecoverException(e, CacheErrors.ERROR_1021);
        }
    }
    
    /**
     * Remove o valor associado à chave.
     * @param key chave associada ao valor.
     * @return <code>true</code> se o valor for removido. Caso contrário <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
     */
    protected boolean removeStream(String key) throws StorageException{
        
        try{
        	DataMap data = this.dataMap.get(key);

            if(data != null){
            	this.remove(key, data);
            	return true;
            }
            else
                return false;
        }
        catch(Throwable e){
            throw new StorageException(e, CacheErrors.ERROR_1022);
        }
        
    }
    
    
    /**
     * Verifica se uma chave está associada a um valor.
     * @param key chave associada ao valor.
     * @return <code>true</code> se a chave estiver associada a um valor. Caso contrário, <code>false</code>.
     */
    public boolean containsKey(String key){
    	return this.dataMap.containsKey(key);
    }
    
    private void remove(String key, DataMap data){
    	if(this.dataMap.remove(key, data)){
	    	this.releaseSegments(data);
	        countRemoved++;
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
                    throw new StorageException(CacheErrors.ERROR_1007);
               
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
            throw new StorageException(e, CacheErrors.ERROR_1014);
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
    
    /**
     * Verifica se os arquivos contidos na pasta de dados serão 
     * destruidos junto com essa instância. 
     * @return <code>true</code> para destruir todos os arquivos. 
     * Caso contrário, <code>false</code>.
     */
    public boolean isDeleteOnExit() {
		return deleteOnExit;
	}

    /**
     * Define que os arquivos contidos na pasta de dados sejam 
     * destruidos junto com essa instância. 
     * @param deleteOnExit <code>true</code> para destruir todos os arquivos. 
     * Caso contrário, <code>false</code>.
     */
	public void setDeleteOnExit(boolean deleteOnExit) {
		this.deleteOnExit = deleteOnExit;
	}

	/**
	 * Remove todas as entradas contidas no cache.
	 */
	public void clear(){
		this.countRead 			= 0;
		this.countReadData 		= 0;
		this.countRemoved 		= 0;
		this.countRemovedData 	= 0;
		this.countWrite 		= 0;
		this.countWriteData 	= 0;
		this.dataList.clear();
		this.dataMap.clear();
		this.freeSegments.clear();
	}
	
	/**
	 * Destrói os dados contidos nesta instância. Deve ser executado se {@link #isDeleteOnExit()} 
	 * for <code>false</code>.
	 */
	public void destroy(){
		this.dataList.destroy();
		this.dataMap.destroy();
		this.freeSegments.clear();
		this.deleteDir(new File(dataPath));
	}
	
	private boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }
    
    protected void finalize() throws Throwable{
    	try{
    		if(this.deleteOnExit){
    			this.destroy();
    		}
    	}
    	finally{
    		super.finalize();
    	}
    }
    
}
