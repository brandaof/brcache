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

import org.brandao.brcache.HugeListCalculator.HugeListInfo;
import org.brandao.brcache.collections.BasicMapReferenceCollection;
import org.brandao.brcache.collections.FlushableReferenceCollection;
import org.brandao.brcache.collections.FlushableReferenceCollectionImp;
import org.brandao.brcache.collections.MapReferenceCollection;
import org.brandao.brcache.collections.swapper.BasicEntityFileSwapper;
import org.brandao.brcache.collections.treehugemap.CharNode;
import org.brandao.brcache.collections.treehugemap.StringTreeNodes;
import org.brandao.brcache.collections.treehugemap.TreeNode;
import org.brandao.brcache.memory.Memory;
import org.brandao.brcache.memory.RegionMemory;
import org.brandao.entityfilemanager.EntityFileManager;

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
 *    cache.putStream("uma_chave", valueStream, 1200, 0);
 *    
 * ex2:
 *    
 *    InputStream value = cache.getStream("uma_chave");
 *
 * </pre>
 * 
 * @author Brandao
 */
public abstract class AbstractCache implements Serializable{
    
    private static final long serialVersionUID = 8023029671447700902L;

    private static final int ENTRY_BINARY_SIZE = 48;
    
    private static final int NODE_BINARY_SIZE = CharNode.DATA_SIZE + ENTRY_BINARY_SIZE;

    private static final int INDEX_BINARY_SIZE = 58 + ENTRY_BINARY_SIZE;
    
    private static final Class<?> ITEM_CACHE_INPUTSTREAM_CLASS = ItemCacheInputStream.class;
    
    private Memory memory;
    
    protected MapReferenceCollection<String, DataMap> dataMap;

    protected FlushableReferenceCollection<Block> dataList;
    
    private int segmentSize;
    
    private long maxBytesToStorageEntry;
    
    private int maxLengthKey;
    
    private volatile long modCount;
    
    volatile long countRead;
    
    volatile long countWrite;
    
    volatile long countRemoved;
    
    volatile long countReadData;

    volatile long countWriteData;

    volatile long countRemovedData;
    
    private boolean deleteOnExit;
    
    public AbstractCache(){
        this.dataMap 				= null;
        this.dataList 				= null;
        this.segmentSize 			= -1;
        this.maxBytesToStorageEntry = -1;
        this.maxLengthKey 			= -1;
    }
    
    /**
     * Cria um novo cache.
     * 
     * @param name Nome do cache.
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
     * @param quantitySwaperThread Quantidade de processos usados para fazer a permuta.
     * @param memory Acesso à memória.
     * @param efm Sistema de arquivos usado pelo cache.
     */
    public AbstractCache(
    		String name,
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
            int quantitySwaperThread,
            Memory memory,
            EntityFileManager efm
    		){
    	this.init(name, nodeBufferSize, nodePageSize, nodeSwapFactor, indexBufferSize, 
    			indexPageSize, indexSwapFactor, dataBufferSize, dataPageSize, blockSize, 
    			dataSwapFactor, maxSizeEntry, maxSizeKey, quantitySwaperThread, memory, efm);
    }

    /**
     * Inicia o cache.
     * 
     * @param name Nome do cache.
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
     * @param quantitySwaperThread Quantidade de processos usados para fazer a permuta.
     * @param memory Acesso à memória.
     * @param efm Sistema de arquivos usado pelo cache.
     */
    protected void init(
    		String name, 
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
            int quantitySwaperThread,
            Memory memory,
            EntityFileManager efm
    		){

    	this.memory                 = memory;
        this.modCount               = 0;
        this.segmentSize            = (int)blockSize;
        this.maxBytesToStorageEntry = maxSizeEntry;
        this.maxLengthKey           = maxSizeKey;
        this.deleteOnExit           = true;
    	
    	HugeListInfo nodeInfo;
    	HugeListInfo indexInfo;
    	HugeListInfo dataInfo; 
        	
    	try{
	    	dataInfo = 
	    			HugeListCalculator
	    				.calculate(dataBufferSize, dataPageSize, blockSize, dataSwapFactor);
	        this.dataList =
	                new FlushableReferenceCollectionImp<Block>(
	                dataInfo.getMaxCapacityElements(),
	                dataInfo.getClearFactorElements(),
	                dataInfo.getFragmentFactorElements(),
	                new BasicEntityFileSwapper<Block>(efm, name + "#dta"),
	                quantitySwaperThread,
	                dataInfo.getSubLists()
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
            		new BasicMapReferenceCollection<String, DataMap>(
                            nodeInfo.getMaxCapacityElements(),
                            nodeInfo.getClearFactorElements(),
                            nodeInfo.getFragmentFactorElements(),
                            new BasicEntityFileSwapper<TreeNode<DataMap>>(efm, name + "#idx"),
                            quantitySwaperThread, 
                            nodeInfo.getSubLists(), 
                            indexInfo.getMaxCapacityElements(),
                            indexInfo.getClearFactorElements(),
                            indexInfo.getFragmentFactorElements(),
                            new BasicEntityFileSwapper<DataMap>(efm, name + "#idx_vla"),
                            quantitySwaperThread, 
                            indexInfo.getSubLists(), 
                            new StringTreeNodes<DataMap>()
    				);
            		
            		
	        this.dataMap.setDeleteOnExit(false);
    	}
    	catch(IllegalArgumentException e){
    		throw new IllegalArgumentException("fail data map", e);
    	}
	    	
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
    protected boolean putStream(String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
        
    	if(timeToLive < 0)
            throw new StorageException(CacheErrors.ERROR_1029);

    	if(timeToIdle < 0)
            throw new StorageException(CacheErrors.ERROR_1028);
    	
        if(key.length() > this.maxLengthKey)
            throw new StorageException(CacheErrors.ERROR_1008);
        
        DataMap oldMap = null;
        DataMap map    = new DataMap();
        
    	//ItemCacheInputStream permite manipular além dos dados os metadados do item.
        if(ITEM_CACHE_INPUTSTREAM_CLASS.isAssignableFrom(inputData.getClass())){
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
        
        //Todo item inserido tem que ter uma nova id. Mesmo que ela exista.
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
            oldMap = this.dataMap.put(key, map);
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
	            this.countRemoved++;
	    	}
        }
        
        this.countWrite++;
        return oldMap != null;
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
    protected boolean replaceStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
        
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
     * Associa o fluxo de bytes do valor à chave somente se a chave não estiver associada a um valor.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return fluxo associado à chave ou <code>null</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item ou se o item atual 
     * expirar no momento da execução do método.
     */
    protected InputStream putIfAbsentStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
        
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
        	in = this.getStream(key, oldMap);
        }
        else{
        	this.countWrite++;
        }
        
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
    	return map == null? null : this.getStream(key, map);
    }
    
    private InputStream getStream(String key, DataMap map) throws RecoverException {
        
        try{
            countRead++;

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
        catch(CorruptedDataException e){
            //e.printStackTrace();
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
    	return this.dataMap.get(key) != null;
    }
    
    private void remove(String key, DataMap data){
    	if(this.dataMap.remove(key, data)){
	    	this.releaseSegments(data);
	        countRemoved++;
    	}
    }
    
    private void putData(DataMap map, InputStream inputData) throws StorageException, InterruptedException{
        
        int writeData    = 0;
        byte[] buffer    = new byte[this.segmentSize];
        int index        = 0;
        Block lastBlock  = null;
        long lastSegment = -1;
        int read;
        
        try{
            while((read = inputData.read(buffer, 0, this.segmentSize)) != -1){
            	writeData += read;
            	
        		RegionMemory data = this.memory.alloc(this.segmentSize);
        		data.write(0, buffer, 0, read);
        		
            	Block block = new Block(map.getId(), index++, data, read);
                Long segment = this.dataList.insert(block);
                
                if(lastBlock != null){
                	lastBlock.nextBlock = segment;
                	this.dataList.set(lastSegment, lastBlock);
                }
                else
                	map.setFirstSegment(segment);
                
            	lastBlock   = block;
                lastSegment = segment;
            }

            this.countWriteData += writeData;
            
    		if(writeData > this.maxBytesToStorageEntry){
                throw new StorageException(CacheErrors.ERROR_1007);
    		}
            
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
    }

    private void releaseSegments(DataMap map){
    	long segmentId = map.getFirstSegment();
    	
    	if(segmentId == -1)
    		return;
    	
        Block current = this.dataList.get(segmentId);
        
        int i=0;
        while(current != null){
			if(current.id == map.getId() && current.segment == i){
				this.dataList.remove(segmentId, current);
			}
            
			segmentId = current.nextBlock;
        	current = segmentId < 0? null : this.dataList.get(segmentId);
        	i++;
        }
    	
    	map.setFirstSegment(-1);
    }
    
    /**
     * Obtém a quantidade de itens recuperados.
     * @return Quantidade de itens recuperados.
     */
    public long getCountRead(){
        return this.countRead;
    }

    /**
     * Obtém a quantidade de itens armazenados.
     * @return Quantidade de itens armazenados.
     */
    public long getCountWrite(){
        return this.countWrite;
    }

    /**
     * Obtém a quantidade de itens removidos.
     * @return Quantidade de itens removidos.
     */
    public long getCountRemoved() {
		return countRemoved;
	}

    /**
     * Obtém a quantidade de bytes recuperados.
     * @return Quantidade de bytes recuperados.
     */
    public long getCountReadData() {
        return countReadData;
    }
    
    /**
     * Obtém a quantidade de bytes armazenados.
     * @return Quantidade de bytes armazenados.
     */
    public long getCountWriteData() {
        return countWriteData;
    }

    /**
     * Obtém a quantidade de bytes removidos.
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
	}
	
	/**
	 * Destrói os dados contidos nesta instância. Deve ser executado se {@link #isDeleteOnExit()} 
	 * for <code>false</code>.
	 */
	public void destroy(){
		this.dataList.destroy();
		this.dataMap.destroy();
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
