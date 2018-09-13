package org.brandao.brcache.tx;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.BRCacheConfig;
import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.CacheHandler;
import org.brandao.brcache.DataMap;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

public class TransactionInfo implements TransactionCacheHandler {

	private static final long serialVersionUID = 3758041685386590737L;
	
	private Serializable id;
	
	private CacheTransactionManager manager;
	
	private CacheHandler cache;
	
	/**
	 * Contém as chaves gerenciadas pela transação.
	 */
	private Set<String> managed;
	
	private Map<String, DataMap> cacheItemMetadata;
	
	private Map<String, DataMap> originalMetadata;
	
	private long timeout;
	
	public TransactionInfo(Serializable id, CacheTransactionManager manager, CacheHandler cache, long timeout){
		this.id                = id;
		this.cache             = cache;
		this.manager           = manager;
		this.managed           = new HashSet<String>();
		this.cacheItemMetadata = new HashMap<String, DataMap>();
		this.originalMetadata  = new HashMap<String, DataMap>();
		this.timeout           = timeout;
	}
	
	/* métodos de armazenamento */
	
	public boolean replaceStream(String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{

		try{
			DataMap dta = getEntity(manager, cache, key, true);
			
			if(dta != null){
				putEntity(manager, cache, key, dta, inputData, timeToLive, timeToIdle);
				return true;
			}
			else
				return false;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
		
	}
	
	public InputStream putIfAbsentStream(String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
		
		try{
			DataMap dta = getEntity(manager, cache, key, true);
			
			if(dta == null){
				putEntity(manager, cache, key, dta, inputData, timeToLive, timeToIdle);
				return null;
			}
			
			return cache.getStream(key, dta);
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}		
	}
	
    public boolean putStream(String key, InputStream inputData, long timeToLive, long timeToIdle) 
    		throws StorageException {

    	try{
    		DataMap o = getEntity(manager, cache, key, true);
    		putEntity(manager, cache, key, o, inputData, timeToLive, timeToIdle);
    		return o != null;
    	}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
    }
	
	/* métodos de coleta*/
	
	public InputStream getStream(String key) throws RecoverException {
		return getStream(key, false);
	}
    
    public InputStream getStream(String key, boolean forUpdate) throws RecoverException {
    	
    	try{
			DataMap dta = getEntity(manager, cache, key, forUpdate);
			return dta != null? cache.getStream(key, dta) : null;
    	}
    	catch(RecoverException e){
    		throw e;
    	}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
    }

    /* métodos de remoção */
    
    public boolean removeStream(String key) throws StorageException{
    	
    	try{
    		DataMap o = getEntity(manager, cache, key, true);
    		putEntity(manager, cache, key, o, null, 0, 0);
    		return o != null;
    	}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
    }
	
	public boolean containsKey(String key) {
		// TODO Auto-generated method stub
		return false;
	}

	public DataMap getPointer(String key) throws RecoverException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setPointer(String key, DataMap newDta) throws RecoverException {
		// TODO Auto-generated method stub
		
	}

	public boolean replacePointer(String key, DataMap originalDta,
			DataMap newDta) throws RecoverException {
		// TODO Auto-generated method stub
		return false;
	}

	public void remove(String key, DataMap data) {
		// TODO Auto-generated method stub
		
	}

	public void releaseSegments(DataMap map) {
		// TODO Auto-generated method stub
		
	}

	public InputStream getStream(String key, DataMap map)
			throws RecoverException {
		// TODO Auto-generated method stub
		return null;
	}

	public void putData(DataMap map, InputStream inputData)
			throws StorageException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	public long getNextModCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public BRCacheConfig getConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	public long getCountRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountWrite() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountRemoved() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountReadData() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountWriteData() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountRemovedData() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isDeleteOnExit() {
		// TODO Auto-generated method stub
		return false;
	}

	public void setDeleteOnExit(boolean deleteOnExit) {
		// TODO Auto-generated method stub
		
	}

	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public void clear() {
		// TODO Auto-generated method stub
		
	}

	public void destroy() {
		// TODO Auto-generated method stub
		
	}
    
    /*métodos de manipulação*/
    
	public void rollback(CacheHandler cache) throws StorageException, RecoverException {
		
		for(Entry<String,DataMap> e: originalMetadata.entrySet()){
			
			DataMap dta = cacheItemMetadata.get(e.getKey());
			
			if(e.getValue() == null){
				if(dta != null){
					cache.remove(e.getKey(), dta);
				}
			}
			else{
				if(dta != null){
					cache.replacePointer(e.getKey(), dta, e.getValue());
					cache.releaseSegments(dta);
				}
				else{
					cache.setPointer(e.getKey(), e.getValue());
				}
			}
		}
		
	}
	
	public void commit(CacheHandler cache) throws RecoverException, StorageException {
		for(Entry<String,DataMap> e: cacheItemMetadata.entrySet()){
			
			DataMap dta = originalMetadata.get(e.getKey());
			
			if(e.getValue() == null){
				if(dta != null){
					cache.remove(e.getKey(), dta);
				}
			}
			else{
				if(dta != null){
					cache.replacePointer(e.getKey(), dta, e.getValue());
					cache.releaseSegments(dta);
				}
				else{
					cache.setPointer(e.getKey(), e.getValue());
				}
			}
		}
		
	}
	
	public void close(CacheHandler cache) throws TransactionException{
		managed.clear();
		cacheItemMetadata.clear();
		cacheItemMetadata.clear();
		originalMetadata.clear();
	}
	
    /* métodos internos */
    
    private DataMap getEntity(CacheTransactionManager manager, CacheHandler cache,
    		String key, boolean lock) 
    		throws RecoverException, IOException, TransactionException{
    	
    	if(lock && !managed.contains(key)){
			manageItem(manager, cache, key, timeout);
    	}
    	
    	return cacheItemMetadata.containsKey(key)? 
    				cacheItemMetadata.get(key) : 
					cache.getPointer(key);
    	
    }

    private void putEntity(
    		CacheTransactionManager manager, CacheHandler cache, String key, DataMap originalDta, 
    		InputStream inputData, long timeToLive, long timeToIdle
    		) throws StorageException, InterruptedException{

		manageItem(manager, cache, key, timeout);
    	
		DataMap newDta = null;
		DataMap oldDta = cacheItemMetadata.get(key);
		
		if(oldDta != null){
			cache.releaseSegments(oldDta);
		}
		
		if(inputData != null){
			newDta = new DataMap();
			newDta.setCreationTime(System.currentTimeMillis());
			newDta.setId(cache.getNextModCount());
			newDta.setMostRecentTime(System.currentTimeMillis());
			newDta.setTimeToIdle(timeToIdle);
			newDta.setTimeToLive(timeToLive);
			cache.putData(newDta, inputData);
		}
		
		cacheItemMetadata.put(key, newDta);
		
    	if(!originalMetadata.containsKey(key)){
			originalMetadata.put(key, originalDta);
    	}
    	
    }
    
    private void manageItem(CacheTransactionManager manager, CacheHandler cache, String key, long time){
    	
    	if(managed.contains(key)){
    		return;
    	}
    	
    	if(time <= 0){
    		manager.lock(id, key);
    	}
    	else{
			manager.tryLock(id, key, time, TimeUnit.MILLISECONDS);
    	}
    	
    	managed.add(key);
    }
    
}
