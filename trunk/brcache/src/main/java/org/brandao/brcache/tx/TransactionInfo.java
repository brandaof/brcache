package org.brandao.brcache.tx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.brandao.brcache.Cache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

public class TransactionInfo {

	private UUID id;
	
	private Set<String> inserted;

	private Set<String> managed;
	
	private Map<String, EntryCache> entities;
	
	private Map<String, EntryCache> saved;
	
	public TransactionInfo(UUID id){
		this.id       = id;
		this.inserted = new HashSet<String>();
		this.managed  = new HashSet<String>();
		this.entities = new HashMap<String, EntryCache>();
	}
	
    public void putObject(String key, long maxAliveTime, 
    		Object item) throws StorageException, TransactionException{
    	throw new UnsupportedOperationException();
    }

    public Object getObject(String key, boolean forUpdate, 
    		long time) throws RecoverException, TransactionException{
    	throw new UnsupportedOperationException();
    }

    public void put(CacheTransactionManager manager, Cache cache, 
    		String key, long maxAliveTime, InputStream inputData) 
    		throws StorageException, TransactionException{
    	
    	try{
    		manager.lock(this.id, key);
    		
    		byte[] dta = inputData == null? null : this.getBytes(inputData);
    		
			this.managed.add(key);
			this.inserted.add(key);
			this.entities.put(key, new EntryCache(dta, maxAliveTime));
    	}
    	catch(TransactionException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new TransactionException(e);
    	}
    }

    public InputStream get(CacheTransactionManager manager, Cache cache, 
    		String key, boolean forUpdate, long time) throws TransactionException{
    	try{
    		byte[] dta = this.getEntity(manager, cache, key, forUpdate, time);
    		return dta == null? null : new ByteArrayInputStream(dta);
    	}
    	catch(TransactionException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new TransactionException(e);
    	}
    }

    private byte[] getEntity(CacheTransactionManager manager, Cache cache,
    		String key, boolean lock, long time) 
    		throws RecoverException, IOException, TransactionException{
    	
    	if(this.managed.contains(key)){
    		EntryCache entry = this.entities.get(key);
    		return entry.getData();
    	}
    	else{
    		byte[] dta = this.getSharedEntity(manager, cache, key, lock, time);
			this.managed.add(key);
			this.entities.put(key, new EntryCache(dta, -1));
    		
    		return dta;
    	}
    }
    
    private byte[] getSharedEntity(CacheTransactionManager manager, Cache cache,
    		String key, boolean lock, long time) 
    		throws IOException, TransactionException, RecoverException{
    	
		if(lock){
			if(time <= 0){
				manager.lock(this.id, key);
			}
			else{
				manager.tryLock(this.id, key, time);
			}
		}
		
		InputStream in = cache.get(key);
		
		if(in != null){
			byte[] dta = this.getBytes(in);
			return dta;
		}
		
		return in == null? null : this.getBytes(in);
    }
    
    private byte[] getBytes(InputStream in) throws IOException{
    	ByteArrayOutputStream bout = new ByteArrayOutputStream(2048);
    	int l = 0;
    	byte[] buffer = new byte[2048];
    	
    	while((l = in.read(buffer, 0, buffer.length)) > 0){
    		bout.write(buffer, 0, l);
    	}
    	bout.close();
    	return bout.toByteArray();
    }
    
    public boolean remove(CacheTransactionManager manager, Cache cache,
    		String key) throws RecoverException, TransactionException{       
    	try{
    		manager.lock(this.id, key);
			this.managed.add(key);
			this.inserted.add(key);
			this.entities.put(key, null);
			return cache.get(key) != null;
    	}
    	catch(TransactionException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new TransactionException(e);
    	}
    }

	public void savePoint(Cache cache) throws IOException, RecoverException{
		saved.clear();

		for(String key: this.inserted){
			InputStream in = cache.get(key);
			if(in != null){
				saved.put(key, new EntryCache(this.getBytes(in), -1));
			}
			else{
				saved.put(key, null);
			}
		}
		
	}
    
	public void rollback(Cache cache) throws StorageException, RecoverException {
		
		for(String key: this.saved.keySet()){
			EntryCache entity = saved.get(key);
			if(entity == null){
				cache.remove(key);
			}
			else{
				cache.put(key, entity.getMaxAlive(), new ByteArrayInputStream(entity.getData()));
			}
		}
		
	}
	
	protected void commit(Cache cache) throws RecoverException, StorageException {
		if(!this.inserted.isEmpty()){
			for(String key: this.inserted){
				EntryCache entity = this.entities.get(key);
				
				if(entity == null){
					cache.remove(key);
				}
				else{
					cache.put(key, entity.getMaxAlive(), new ByteArrayInputStream(entity.getData()));
				}
			}
			
		}
	}
	
	public void clear(){
		this.entities.clear();
		this.inserted.clear();
		this.saved.clear();
	}
	
}
