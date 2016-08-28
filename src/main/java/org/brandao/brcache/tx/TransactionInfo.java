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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.Cache;
import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.CacheInputStream;
import org.brandao.brcache.ItemCacheInputStream;
import org.brandao.brcache.ItemCacheMetadata;
import org.brandao.brcache.StreamCache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

public class TransactionInfo implements Serializable {

	private static final long serialVersionUID = 3758041685386590737L;

	private static final String ORIGIN_PREFIX = "o:";
	
	private UUID id;
	
	private Set<String> updated;

	//private Set<String> locked;
	
	/**
	 * Contém as chaves gerenciadas pela transação.
	 */
	private Set<String> managed;
	
	private Map<String, ItemCacheMetadata> cacheItemMetadata;
	
	private StreamCache entities;
	
	private Set<String> saved;
	
	public TransactionInfo(UUID id,
			BRCacheTransactionConfig cacheTransactionConfig){
		this.id                = id;
		this.updated           = new HashSet<String>();
		//this.locked            = new HashSet<String>();
		this.managed           = new HashSet<String>();
		this.cacheItemMetadata = new HashMap<String, ItemCacheMetadata>();
		this.saved             =  new HashSet<String>();
		
		this.entities = 
				new Cache(
					new BRCacheTransactionConfigWrapper(
						cacheTransactionConfig,
						cacheTransactionConfig.getDataPath() + "/" + id.toString()
					)
				);
		
		this.entities.setDeleteOnExit(false);
	}
	
	/* métodos de armazenamento */
	
	public Object replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long timeToLive, long timeToIdle, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			if(o != null){
				this.put(manager, cache, key, value, timeToLive, timeToIdle, time);
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
	
	public boolean replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object oldValue, 
			Object newValue, long timeToLive, long timeToIdle, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			if(o != null && o.equals(oldValue)){
				this.put(manager, cache, key, newValue, timeToLive, timeToIdle, time);
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
	
	public Object putIfAbsent(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long timeToLive, long timeToIdle, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			
			if(o == null){
				this.put(manager, cache, key, value, timeToLive, timeToIdle, time);
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
	
	public void put(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long timeToLive, long timeToIdle, long time) throws StorageException {
		try{
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
			oout.close();
			this.putStream(
				manager, cache, key, timeToLive, timeToIdle, 
				new ByteArrayInputStream(bout.toByteArray()), time);
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
    public void putStream(CacheTransactionManager manager, StreamCache cache, 
    		String key, long timeToLive, long timeToIdle, InputStream inputData, long time) 
    		throws StorageException {

    	try{
    		this.manageItem(manager, cache, key, time);
			this.entities.putStream(key, 0, 0, inputData);
			this.updated.add(key);
			this.cacheItemMetadata.put(key, new ItemCacheMetadata
					(-1, timeToLive, timeToIdle, -1, -1, -1, -1, (short)-1, -1));
    	}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
    }
	
	/* métodos de coleta*/
	
	public Object get(CacheTransactionManager manager, StreamCache cache,
			String key, boolean forUpdate, long time) throws RecoverException {
		try{
			InputStream in = this.getStream(manager, cache, key, forUpdate, time);
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
    
    public InputStream getStream(CacheTransactionManager manager, StreamCache cache, 
    		String key, boolean forUpdate, long time) throws RecoverException {
    	
    	try{
			InputStream dta = this.getEntity(manager, cache, key, forUpdate, time);
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
    
	public boolean remove(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			if(o != null && o.equals(value)){
				return this.remove(manager, cache, key, time);
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
	
    public boolean remove(CacheTransactionManager manager, StreamCache cache,
    		String key, long time) throws StorageException{       
    	try{
    		if(this.managed.contains(key)){
    			this.updated.add(key);
    			return this.entities.remove(key);
    		}
    		else{
        		this.manageItem(manager, cache, key, time);
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
    
	public void savePoint(StreamCache cache) throws IOException, RecoverException{
		saved.clear();

		for(String key: this.updated){
			CacheInputStream in = (CacheInputStream) cache.getStream(key);
			
			if(in != null){
				String orgKey = ORIGIN_PREFIX + key;
				//Se for usar o cache raiz tem que colocar o tempo do timeout da transação.
				this.entities.putStream(orgKey, 0, 0, in); 
				this.cacheItemMetadata.put(orgKey, new ItemCacheMetadata(in));
				saved.add(key);
			}
			else{
				saved.add(key);
			}
		}
		
	}
    
	public void rollback(StreamCache cache) throws StorageException, RecoverException {
		
		for(String key: this.saved){
			
			String orgKey = ORIGIN_PREFIX + key;
			InputStream in = this.entities.getStream(orgKey);
			
			if(in == null){
				cache.remove(key);
			}
			else{
				ItemCacheMetadata metadata = this.cacheItemMetadata.get(orgKey);
				ItemCacheInputStream item  = new ItemCacheInputStream(metadata, in);
				cache.putStream(key, -1, -1, item);
			}
		}
		
	}
	
	public void commit(StreamCache cache) throws RecoverException, StorageException {
		if(!this.updated.isEmpty()){
			for(String key: this.updated){
				CacheInputStream entity = (CacheInputStream) this.entities.getStream(key);
				
				if(entity == null){
					cache.remove(key);
				}
				else{
					ItemCacheMetadata metadata = this.cacheItemMetadata.get(key);
					ItemCacheInputStream in = new ItemCacheInputStream(
							-1, metadata.getTimeToLive(), metadata.getTimeToIdle(), 
							-1, -1, entity.getFlag(), entity.getSize(), entity);
					cache.putStream(key, -1, -1, in);
				}
			}
			
		}
	}
	
	public void close() throws TransactionException{
		this.entities.destroy();
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
    
    private InputStream getEntity(CacheTransactionManager manager, StreamCache cache,
    		String key, boolean lock, long time) 
    		throws RecoverException, IOException, TransactionException{
    	
    	if(this.managed.contains(key)){
    		InputStream entry = this.entities.getStream(key);
    		return entry;
    	}
    	else{
			this.manageItem(manager, cache, key, time);
    		CacheInputStream dta = (CacheInputStream) this.getSharedEntity(manager, cache, key, lock);
			
			if(dta != null){
				this.entities.putStream(key, 0, 0, dta);
				this.cacheItemMetadata.put(key, new ItemCacheMetadata(dta));
				return this.entities.getStream(key);
			}
			else
				return dta;
    	}
    }
    
    private void manageItem(CacheTransactionManager manager, StreamCache cache, String key, long time){
    	
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

    
    private InputStream getSharedEntity(CacheTransactionManager manager, StreamCache cache,
    		String key, boolean lock) 
    		throws IOException, TransactionException, RecoverException{
    	
		InputStream in = cache.getStream(key);
		return in;
    }
    
    private byte[] getBytes(InputStream in) throws IOException {
    	ByteArrayOutputStream bout = new ByteArrayOutputStream(2048);
    	int l = 0;
    	byte[] buffer = new byte[2048];
    	
    	while((l = in.read(buffer, 0, buffer.length)) > 0){
    		bout.write(buffer, 0, l);
    	}
    	bout.close();
    	return bout.toByteArray();
    }
	
}
