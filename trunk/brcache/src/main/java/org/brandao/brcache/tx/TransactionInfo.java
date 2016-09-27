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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.BasicCache;
import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.CacheInputStream;
import org.brandao.brcache.ItemCacheInputStream;
import org.brandao.brcache.ItemCacheMetadata;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

class TransactionInfo implements Serializable {

	private static final long serialVersionUID = 3758041685386590737L;

	private static final String ORIGINAL_PREFIX  = "t:o:{{name}}:";

	private static final String DATA_PREFIX = "t:{{name}}:";
	
	private Serializable id;
	
	private Set<String> updated;

	/**
	 * Contém as chaves gerenciadas pela transação.
	 */
	private Set<String> managed;
	
	private Map<String, ItemCacheMetadata> cacheItemMetadata;
	
	private Set<String> saved;
	
	private long timeout;
	
	private String originalDataPrefix;

	private String dataPrefix;
	
	private String name;
	
	public TransactionInfo(Serializable id, long timeout){
		this.id                 = id;
		this.name               = id.toString();
		this.updated            = new HashSet<String>();
		this.managed            = new HashSet<String>();
		this.cacheItemMetadata  = new HashMap<String, ItemCacheMetadata>();
		this.saved              = new HashSet<String>();
		this.timeout            = timeout;
		this.originalDataPrefix = ORIGINAL_PREFIX.replace("{{name}}", this.name);
		this.dataPrefix         = DATA_PREFIX.replace("{{name}}", this.name);
	}
	
	/* métodos de armazenamento */
	
	public boolean replace(CacheTransactionManager manager, BasicCache cache,
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true);
			if(o != null){
				this.put(manager, cache, key, value, timeToLive, timeToIdle);
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
	
	public boolean replaceStream(CacheTransactionManager manager, BasicCache cache,
			String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{

		try{
			Object o = this.getStream(manager, cache, key, true);
			if(o != null){
				this.putStream(manager, cache, key, inputData, timeToLive, timeToIdle);
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
	
	public boolean replace(CacheTransactionManager manager, BasicCache cache,
			String key, Object oldValue, 
			Object newValue, long timeToLive, long timeToIdle) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true);
			if(o != null && o.equals(oldValue)){
				this.put(manager, cache, key, newValue, timeToLive, timeToIdle);
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
	
	public Object putIfAbsent(CacheTransactionManager manager, BasicCache cache,
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true);
			
			if(o == null){
				this.put(manager, cache, key, value, timeToLive, timeToIdle);
			}
			
			return o;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
	public InputStream putIfAbsentStream(CacheTransactionManager manager, BasicCache cache,
			String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
		
		try{
			InputStream o = this.getStream(manager, cache, key, true);
			
			if(o == null){
				this.putStream(manager, cache, key, inputData, timeToLive, timeToIdle);
			}
			
			return o;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}		
	}
	
	
	public boolean put(CacheTransactionManager manager, BasicCache cache,
			String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		try{
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
			oout.close();
			return this.putStream(
				manager, cache, key, new ByteArrayInputStream(bout.toByteArray()), 
				timeToLive, timeToIdle);
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
    public boolean putStream(CacheTransactionManager manager, BasicCache cache, 
    		String key, InputStream inputData, long timeToLive, long timeToIdle) 
    		throws StorageException {

    	String ref = this.dataPrefix + key;
    	try{
    		if(!this.managed.contains(key)){
        		this.manageItem(manager, cache, key, this.timeout);
        		cache.putStream(ref, inputData, this.timeout, 0);
    			this.updated.add(key);
    			this.cacheItemMetadata.put(key, new ItemCacheMetadata
    					(-1, timeToLive, timeToIdle, -1, -1, -1, -1, (short)-1, -1));
    			return cache.containsKey(key);
    		}
    		else{
				this.updated.add(key);
				this.cacheItemMetadata.put(key, new ItemCacheMetadata
						(-1, timeToLive, timeToIdle, -1, -1, -1, -1, (short)-1, -1));
				return cache.putStream(ref, inputData, this.timeout, 0);
    		}
    	}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
    }
	
	/* métodos de coleta*/
	
	public Object get(CacheTransactionManager manager, BasicCache cache,
			String key, boolean forUpdate) throws RecoverException {
		try{
			InputStream in = this.getStream(manager, cache, key, forUpdate);
			if(in != null){
				ObjectInputStream oin = new ObjectInputStream(in);
				return oin.readObject();
			}
			else
				return null;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
	}
    
    public InputStream getStream(CacheTransactionManager manager, BasicCache cache, 
    		String key, boolean forUpdate) throws RecoverException {
    	
    	try{
			InputStream dta = this.getEntity(manager, cache, key, forUpdate);
			return dta;
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
    
	public boolean remove(CacheTransactionManager manager, BasicCache cache,
			String key, Object value) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true);
			if(o != null && o.equals(value)){
				return this.remove(manager, cache, key);
			}
			else
				return false;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
	}
	
    public boolean remove(CacheTransactionManager manager, BasicCache cache,
    		String key) throws StorageException{
    	
    	String ref = this.dataPrefix + key;
    	try{
    		if(this.managed.contains(key)){
    			this.updated.add(key);
    			return cache.remove(ref);
    		}
    		else{
        		this.manageItem(manager, cache, key, this.timeout);
    			this.updated.add(key);
    			return cache.getStream(key) != null;
    		}
    	}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
    }
	
    /* métodos de manipulação*/
    
	public void savePoint(BasicCache cache) throws IOException, RecoverException{
		saved.clear();

		for(String key: this.updated){
			CacheInputStream in = (CacheInputStream) cache.getStream(key);
			
			if(in != null){
				String orgKey = this.originalDataPrefix + key;
				cache.putStream(orgKey, in, this.timeout, 0); 
				this.cacheItemMetadata.put(orgKey, new ItemCacheMetadata(in));
				saved.add(key);
			}
			else{
				saved.add(key);
			}
		}
		
	}
    
	public void rollback(BasicCache cache) throws StorageException, RecoverException {
		
		for(String key: this.saved){
			
			String orgKey = this.originalDataPrefix + key;
			InputStream in = cache.getStream(orgKey);
			
			if(in == null){
				cache.remove(key);
			}
			else{
				ItemCacheMetadata metadata = this.cacheItemMetadata.get(orgKey);
				ItemCacheInputStream item  = new ItemCacheInputStream(metadata, in);
				cache.putStream(key, item, 0, 0);
			}
		}
		
	}
	
	public void commit(BasicCache cache) throws RecoverException, StorageException {
		if(!this.updated.isEmpty()){
			for(String key: this.updated){
				String ref = this.dataPrefix + key;
				CacheInputStream entity = (CacheInputStream) cache.getStream(ref);
				
				if(entity == null){
					cache.remove(key);
				}
				else{
					ItemCacheMetadata metadata = this.cacheItemMetadata.get(key);
					/*
					ItemCacheInputStream in = new ItemCacheInputStream(
							-1, metadata.getTimeToLive(), metadata.getTimeToIdle(), 
							-1, -1, entity.getFlag(), entity.getSize(), entity);
					*/
					cache.putStream(key, entity, metadata.getTimeToLive(), metadata.getTimeToIdle());
				}
			}
			
		}
	}
	
	public void close(BasicCache cache) throws TransactionException{
		
		for(String key: this.managed){
			String org = this.originalDataPrefix + key;
			String ref = this.dataPrefix + key;
			cache.remove(org);
			cache.remove(ref);
		}
		
		this.managed.clear();
		this.saved.clear();
		this.cacheItemMetadata.clear();
		this.updated.clear();
		
		/*
		this.updated.clear();
		this.locked.clear();
		
		if(!this.managed.isEmpty()){
			for(String key: this.managed){
				this.entities.remove(key);
			}
		}
		
		this.times.clear();
		this.managed.clear();
		this.saved.clear();
		*/
	}
	
    /* métodos internos */
    
    private InputStream getEntity(CacheTransactionManager manager, BasicCache cache,
    		String key, boolean lock) 
    		throws RecoverException, IOException, TransactionException{
    	
    	String ref = this.dataPrefix + key;
    	
    	if(this.managed.contains(key)){
    		InputStream entry = cache.getStream(ref);
    		return entry;
    	}
    	else{
			this.manageItem(manager, cache, key, this.timeout);
    		CacheInputStream dta = (CacheInputStream) this.getSharedEntity(manager, cache, key, lock);
			
			if(dta != null){
				cache.putStream(ref, dta, this.timeout, 0);
				this.cacheItemMetadata.put(key, new ItemCacheMetadata(dta));
				return cache.getStream(ref);
			}
			else
				return dta;
    	}
    }
    
    private void manageItem(CacheTransactionManager manager, BasicCache cache, String key, long time){
    	
    	if(this.managed.contains(key)){
    		return;
    	}
    	
    	if(time <= 0){
    		manager.lock(this.id, key);
    	}
    	else{
			manager.tryLock(this.id, key, time, TimeUnit.MILLISECONDS);
    	}
    	
    	this.managed.add(key);
    }

    
    private InputStream getSharedEntity(CacheTransactionManager manager, BasicCache cache,
    		String key, boolean lock) 
    		throws IOException, TransactionException, RecoverException{
		InputStream in = cache.getStream(key);
		return in;
    }
    
}
