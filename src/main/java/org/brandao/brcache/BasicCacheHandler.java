package org.brandao.brcache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.brandao.brcache.HugeListCalculator.HugeListInfo;
import org.brandao.brcache.collections.BasicMapReferenceCollection;
import org.brandao.brcache.collections.BlockEntityFileDataHandler;
import org.brandao.brcache.collections.BlockEntityFileHeader;
import org.brandao.brcache.collections.CharNodeEntityFileDataHandler;
import org.brandao.brcache.collections.CharNodeEntityFileHeader;
import org.brandao.brcache.collections.DataMapEntityFileDataHandler;
import org.brandao.brcache.collections.DataMapEntityFileHeader;
import org.brandao.brcache.collections.FlushableReferenceCollection;
import org.brandao.brcache.collections.FlushableReferenceCollectionImp;
import org.brandao.brcache.collections.MapReferenceCollection;
import org.brandao.brcache.collections.swapper.BasicEntityFileSwapper;
import org.brandao.brcache.collections.treehugemap.CharNode;
import org.brandao.brcache.collections.treehugemap.StringTreeNodes;
import org.brandao.brcache.collections.treehugemap.TreeNode;
import org.brandao.brcache.memory.Memory;
import org.brandao.brcache.memory.RegionMemory;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.EntityFileManagerImp;
import org.brandao.entityfilemanager.EntityFileTransactionFactory;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.LockProviderImp;
import org.brandao.entityfilemanager.SimpleEntityFileAccess;
import org.brandao.entityfilemanager.TransactionLog;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManagerConfigurer;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManagerImp;
import org.brandao.entityfilemanager.tx.TransactionLogImp;
import org.brandao.entityfilemanager.tx.async.AsyncEntityFileTransactionFactory;
import org.brandao.entityfilemanager.tx.async.AsyncRecoveryTransactionLog;

public class BasicCacheHandler implements CacheHandler{

    private static final long serialVersionUID                 = 8023029671447700902L;

    private static final int ENTRY_BINARY_SIZE                 = 48;
    
    private static final int NODE_BINARY_SIZE                  = CharNode.DATA_SIZE + ENTRY_BINARY_SIZE;

    private static final int INDEX_BINARY_SIZE                 = 58 + ENTRY_BINARY_SIZE;
    
    private static final Class<?> ITEM_CACHE_INPUTSTREAM_CLASS = ItemCacheInputStream.class;
    
    private Memory memory;
    
    protected MapReferenceCollection<String, DataMap> dataMap;

    protected FlushableReferenceCollection<Block> dataList;
    
    private int segmentSize;
    
    private long maxBytesToStorageEntry;
    
    private int maxLengthKey;
    
    private volatile long modCount;
    
    protected EntityFileManagerConfigurer entityFileManager;
    
	protected BRCacheConfig config;
    
    volatile long countRead;
    
    volatile long countWrite;
    
    volatile long countRemoved;
    
    volatile long countReadData;

    volatile long countWriteData;

    volatile long countRemovedData;
    
    private boolean deleteOnExit;
    
    private boolean enabled;
    /**
     * Cria um novo cache.
     * 
     * @param name Nome do cache.
     * @param config Configuração do cache.
     */

    public BasicCacheHandler(String name, BRCacheConfig config) throws CacheException{
    	this.config                 = config;
    	this.memory                 = config.getMemory();
        this.modCount               = 0;
        this.segmentSize            = (int)config.getDataBlockSize();
        this.maxBytesToStorageEntry = config.getMaxSizeEntry();
        this.maxLengthKey           = config.getMaxSizeKey();
        this.deleteOnExit           = true;
    	this.entityFileManager      = this.createEntityFileManager(config);
        this.dataList               = this.createDataBuffer(name, this.entityFileManager, config);
        this.dataMap                = this.createDataMap(name, this.entityFileManager, config);
        this.enabled                = true;
    }
    
    private EntityFileManagerConfigurer createEntityFileManager(BRCacheConfig config){
    	try{
    		if(config.getEntityFileManager() != null){
    			return (EntityFileManagerConfigurer)config.getEntityFileManager();
    		}
    		
			File path   = new File(config.getDataPath());
			File txPath = new File(path, "tx");
			
			EntityFileManagerConfigurer efm           = new EntityFileManagerImp();
			LockProvider lp                           = new LockProviderImp();
			EntityFileTransactionManagerConfigurer tm = new EntityFileTransactionManagerImp();
			AsyncRecoveryTransactionLog rtl           = new AsyncRecoveryTransactionLog("recovery", txPath, tm);
			TransactionLog tl                         = new TransactionLogImp("binlog", txPath, tm);
			EntityFileTransactionFactory eftf         = new AsyncEntityFileTransactionFactory(rtl);
			
			rtl.setForceReload(true);
			
			tm.setTransactionLog(tl);
			tm.setRecoveryTransactionLog(rtl);
			tm.setEntityFileTransactionFactory(eftf);
			tm.setLockProvider(lp);
			tm.setTimeout(EntityFileTransactionManagerImp.DEFAULT_TIMEOUT);
			tm.setTransactionPath(txPath);
			tm.setEntityFileManagerConfigurer(efm);
			tm.setEnabledTransactionLog(false);
			
			efm.setEntityFileTransactionManager(tm);
			efm.setLockProvider(lp);
			efm.setPath(path);
			
			efm.init();
			
			return efm;    	
    	}
    	catch(IllegalArgumentException e){
    		throw new IllegalArgumentException("fail create persistence manager", e);
    	}
    }
    
    private FlushableReferenceCollection<Block> createDataBuffer(String name, 
    		EntityFileManagerConfigurer efm, BRCacheConfig config){
    	try{
	    	HugeListInfo dataInfo = 
	    			HugeListCalculator
	    				.calculate(
	    						config.getDataBufferSize(),
	    						config.getDataPageSize(),
	    						config.getDataBlockSize(),
	    						config.getDataSwapFactor());
	    	
	    	efm.register(new SimpleEntityFileAccess<Block, byte[], BlockEntityFileHeader>(
	    			name + "_dta", 
	    			new File(config.getDataPath(), name + "_dta"), 
	    			new BlockEntityFileDataHandler(this.memory, (int)config.getDataBlockSize())));
	    	efm.truncate(name + "_dta");
	    	
	    	FlushableReferenceCollection<Block> dataList =
	                new FlushableReferenceCollectionImp<Block>(
	                dataInfo.getMaxCapacityElements(),
	                dataInfo.getClearFactorElements(),
	                dataInfo.getFragmentFactorElements(),
	                new BasicEntityFileSwapper<Block>(efm, name + "_dta", Block.class),
	                config.getSwapperThread(),
	                dataInfo.getSubLists()
	                );
	        dataList.setDeleteOnExit(false);
	        
	        return dataList;
    	}
    	catch(IllegalArgumentException e){
    		throw new IllegalArgumentException("fail create data buffer", e);
    	}
    }
    
    @SuppressWarnings("rawtypes")
	private MapReferenceCollection<String, DataMap> createDataMap(
    		String name, EntityFileManagerConfigurer efm, BRCacheConfig config){

    	try{
    		HugeListInfo nodeInfo = 
	    			HugeListCalculator
	    				.calculate(
	    						config.getNodesBufferSize(),
	    						config.getNodesPageSize(),
	    						NODE_BINARY_SIZE, 
	    						config.getNodesSwapFactor());
    		
    		HugeListInfo indexInfo = 
	    			HugeListCalculator
	    				.calculate(
	    						config.getIndexBufferSize(),
	    						config.getIndexPageSize(),
	    						INDEX_BINARY_SIZE, 
	    						config.getIndexSwapFactor());
    		
	    	efm.register(new SimpleEntityFileAccess<CharNode, byte[], CharNodeEntityFileHeader>(
	    			name + "_idx", 
	    			new File(config.getDataPath(), name + "_idx"), 
	    			new CharNodeEntityFileDataHandler()));
	    	efm.truncate(name + "_idx");

	    	efm.register(new SimpleEntityFileAccess<DataMap, byte[], DataMapEntityFileHeader>(
	    			name + "_idxv", 
	    			new File(config.getDataPath(), name + "_idxv"), 
	    			new DataMapEntityFileDataHandler()));
	    	efm.truncate(name + "_idxv");
	    	
    		MapReferenceCollection<String, DataMap> dataMap =
            		new BasicMapReferenceCollection<String, DataMap>(
                            nodeInfo.getMaxCapacityElements(),
                            nodeInfo.getClearFactorElements(),
                            nodeInfo.getFragmentFactorElements(),
                            new BasicEntityFileSwapper<TreeNode<DataMap>>(efm, name + "_idx", TreeNode.class),
                            config.getSwapperThread(), 
                            nodeInfo.getSubLists(), 
                            indexInfo.getMaxCapacityElements(),
                            indexInfo.getClearFactorElements(),
                            indexInfo.getFragmentFactorElements(),
                            new BasicEntityFileSwapper<DataMap>(efm, name + "_idxv", DataMap.class),
                            config.getSwapperThread(), 
                            indexInfo.getSubLists(), 
                            new StringTreeNodes<DataMap>()
    				);
            		
            		
	        dataMap.setDeleteOnExit(false);
	        return dataMap;
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
    public boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
        
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
        map.setId(modCount++);

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
    public boolean replaceStream(String key, InputStream inputData, 
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
    public InputStream putIfAbsentStream(String key, InputStream inputData, 
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
            oldMap = dataMap.putIfAbsent(key, map);
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
    
    public InputStream getStream(String key) throws RecoverException {
        DataMap map = this.dataMap.get(key);
    	return map == null? null : this.getStream(key, map);
    }
    
    /**
     * Remove o valor associado à chave.
     * @param key chave associada ao valor.
     * @return <code>true</code> se o valor for removido. Caso contrário <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
     */
    public boolean removeStream(String key) throws StorageException{
        
        try{
        	DataMap data = this.dataMap.get(key);

            if(data != null){
            	remove(key, data);
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
    	return dataMap.get(key) != null;
    }

    /**
     * Obtém o apontamento da chave.
     * @param key chave.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * apontamento.
     */
    
    public DataMap getPointer(String key) throws RecoverException {
        return dataMap.get(key);
    }

    public void setPointer(String key, DataMap newDta) throws RecoverException {
    	dataMap.put(key, newDta);
    }
    
    public boolean replacePointer(String key, DataMap originalDta, DataMap newDta) throws RecoverException {
    	return dataMap.replace(key, originalDta, newDta);
    }
    
    /**
     * Remove os dados do item a partir de seu apontamento e chave.
     * @param key Chave
     * @param data Apontameto.
     */
    public void remove(String key, DataMap data){
    	if(this.dataMap.remove(key, data)){
	    	this.releaseSegments(data);
	        countRemoved++;
    	}
    }
    
    public void releaseSegments(DataMap map){
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
     * Obtém o fluxo de dados do item a partir de seu apontamento e chave.
     * @param key Chave
     * @param map Apontameto.
     * @return Fluxo de dados.
     * @throws RecoverException Lançada se ocorrer algum problema ao tentar obter os dados.
     */
    public InputStream getStream(String key, DataMap map) throws RecoverException {
        
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
            	dataMap.replace(key, map, map);
        	}
        	
        	int readData     = 0;
            Block[] segments = new Block[map.getSegments()];
            Block current    = dataList.get(map.getFirstSegment());
            int i            = 0;
            
            while(current != null){

                /*
                Se id for diferente da
                id do DataMap, significa que essa entrada foi ou está sendo
                removida.
                */
				if(current.id != map.getId() || current.segment != i)
				    throw new CorruptedDataException("invalid segment: " + current.id + ":" + map.getId() + " " + current.segment + ":" + i);
                
            	readData    += current.length;
                segments[i]  = current;
            	current      = current.nextBlock < 0? null : dataList.get(current.nextBlock);
            	i++;
            }
            
            countReadData += readData;
            
            return new CacheInputStream(this, map, segments);
        }
        catch(CorruptedDataException e){
            return null;
        }
        catch(Throwable e){
            throw new RecoverException(e, CacheErrors.ERROR_1021);
        }
    }
    
    /**
     * Insere o fluxo de dados do item no cache usando um apontamento.
     * @param map Apontameto.
     * @param inputData Fluxo de dados.
     * @throws StorageException Lançada se ocorrer algum problema ao tentar remover os dados.
     */
    public void putData(DataMap map, InputStream inputData) throws StorageException, InterruptedException{
        
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
                Long segment = dataList.insert(block);

                if(lastBlock != null){
                	lastBlock.nextBlock = segment;
                	dataList.set(lastSegment, lastBlock);
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

    public long getNextModCount(){
    	return modCount++;
    }
    
	/**
	 * Obtém a configuração do cache.
	 * @return configuração.
	 */
	public BRCacheConfig getConfig() {
		return config;
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
	 * Obtém a quantidade de itens no cache.
	 * @return Quantidade.
	 */
	public long size() {
		return countRemoved - countWrite;
	}

	public int getMaxKeySize() {
		return maxLengthKey;
	}
	
	/**
	 * Verifica se o cache está vazio.
	 * @return <code>true</code> se o cache estiver vazio.
	 */
	public boolean isEmpty() {
		return (countRemoved - countWrite) == 0;
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
		if(enabled){
			dataList.destroy();
			dataMap.destroy();
			entityFileManager.destroy();
			enabled = false;
		}
	}
	
    protected void finalize() throws Throwable{
    	try{
    		if(deleteOnExit){
    			destroy();
    		}
    	}
    	finally{
    		super.finalize();
    	}
    }
	
}
