package org.brandao.brcache.tx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.BasicCache;
import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.CacheHandler;
import org.brandao.brcache.CacheInputStream;
import org.brandao.brcache.DataMap;
import org.brandao.brcache.ItemCacheInputStream;
import org.brandao.brcache.ItemCacheMetadata;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

class TransactionInfo implements Serializable {

	private static final long serialVersionUID = 3758041685386590737L;
	
	private Serializable id;
	
	private Set<String> updated;

	/**
	 * Contém as chaves gerenciadas pela transação.
	 */
	private Set<String> managed;
	
	private Map<String, DataMap> cacheItemMetadata;
	
	private Map<String, DataMap> originalMetadata;
	
	private long timeout;
	
	private String originalDataPrefix;

	private String dataPrefix;
	
	public TransactionInfo(Serializable id, long timeout){
		this.id                = id;
		this.updated           = new HashSet<String>();
		this.managed           = new HashSet<String>();
		this.cacheItemMetadata = new HashMap<String, DataMap>();
		this.originalMetadata  = new HashMap<String, DataMap>();
		this.timeout           = timeout;
	}
	
	/* métodos de armazenamento */
	
	public boolean replaceStream(CacheTransactionManager manager, CacheHandler cache,
			String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{

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
	
	public InputStream putIfAbsentStream(CacheTransactionManager manager, CacheHandler cache,
			String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
		
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
	
    public boolean putStream(CacheTransactionManager manager, CacheHandler cache, 
    		String key, InputStream inputData, long timeToLive, long timeToIdle) 
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
	
    
    public InputStream getStream(CacheTransactionManager manager, CacheHandler cache, 
    		String key, boolean forUpdate) throws RecoverException {
    	
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
    
    public boolean remove(CacheTransactionManager manager, CacheHandler cache,
    		String key) throws StorageException{
    	
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
	
    /* métodos de manipulação*/
    
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
				}
				else{
					cache.setPointer(e.getKey(), e.getValue());
				}
			}
		}
		
	}
	
	public void commit(CacheHandler cache) throws RecoverException, StorageException {
		if(!this.updated.isEmpty()){
			for(String key: this.updated){
				String ref = this.dataPrefix + key;
				CacheInputStream entity = (CacheInputStream) cache.getStream(ref);
				
				if(entity == null){
					cache.remove(key);
				}
				else{
					ItemCacheMetadata metadata = this.cacheItemMetadata.get(key);
					cache.putStream(key, entity, metadata.getTimeToLive(), metadata.getTimeToIdle());
				}
			}
			
		}
	}
	
	public void close(BasicCache cache) throws TransactionException{
		
		for(String key: this.updated){
			String org = this.originalDataPrefix + key;
			String ref = this.dataPrefix + key;
			cache.remove(org);
			cache.remove(ref);
		}
		
		this.managed.clear();
		this.saved.clear();
		this.cacheItemMetadata.clear();
		this.updated.clear();
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
    	
		DataMap newDta = new DataMap();
		
		newDta.setCreationTime(System.currentTimeMillis());
		newDta.setId(cache.getNextModCount());
		newDta.setMostRecentTime(System.currentTimeMillis());
		newDta.setTimeToIdle(timeToIdle);
		newDta.setTimeToLive(timeToLive);
		
		cache.putData(newDta, inputData);
		
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
