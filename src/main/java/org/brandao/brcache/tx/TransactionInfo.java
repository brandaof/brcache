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

import org.brandao.brcache.StreamCache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

public class TransactionInfo implements Serializable {

	private static final long serialVersionUID = 3758041685386590737L;

	private static final EntryCache EMPTY = new EntryCache(null, -1);
	
	private UUID id;
	
	private Set<String> updated;

	private Set<String> locked;
	
	private Set<String> managed;
	
	private Map<String, EntryCache> entities;
	
	private Map<String, EntryCache> saved;
	
	public TransactionInfo(UUID id){
		this.id       = id;
		this.updated  = new HashSet<String>();
		this.locked   = new HashSet<String>();
		this.managed  = new HashSet<String>();
		this.entities = new HashMap<String, EntryCache>();
		this.saved    = new HashMap<String, EntryCache>();
	}
	
	/* métodos de armazenamento */
	
	public Object replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			if(o != null){
				this.put(manager, cache, key, value, maxAliveTime, time);
				return true;
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	public boolean replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object oldValue, 
			Object newValue, long maxAliveTime, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			if(o != null && o.equals(oldValue)){
				this.put(manager, cache, key, newValue, maxAliveTime, time);
				return true;
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	public Object putIfAbsent(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		
		try{
			Object o = this.get(manager, cache, key, true, time);
			
			if(o == null){
				this.put(manager, cache, key, value, maxAliveTime, time);
			}
			
			return o;
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
	public void put(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long maxAliveTime, long time) throws StorageException {
		try{
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
			this.putStream(
				manager, cache, key, maxAliveTime, 
				new ByteArrayInputStream(bout.toByteArray()), time);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
    public void putStream(CacheTransactionManager manager, StreamCache cache, 
    		String key, long maxAliveTime, InputStream inputData, long time) 
    		throws StorageException {

    	try{
			byte[] dta = 
					inputData == null? 
						null : 
						this.getBytes(inputData);
    		if(this.managed.contains(key)){
    			this.entities.put(key, new EntryCache(dta, maxAliveTime));
    		}
    		else{
    			this.lock(manager, key, time);
    			this.managed.add(key);
    			this.updated.add(key);
    			this.entities.put(key, new EntryCache(dta, maxAliveTime));
    		}
    	}
    	catch(Throwable e){
    		throw new StorageException(e);
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
		catch(RecoverException e){
			throw e;
		}	
		catch(Throwable e){
			throw new RecoverException(e);
		}	
	}
    
    public InputStream getStream(CacheTransactionManager manager, StreamCache cache, 
    		String key, boolean forUpdate, long time) throws RecoverException {
    	
    	try{
			byte[] dta = this.getEntity(manager, cache, key, forUpdate, time);
			return dta == null? null : new ByteArrayInputStream(dta);
    	}
    	catch(RecoverException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new RecoverException(e);
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
		catch(Throwable e){
			throw new StorageException(e);
		}
	}
	
    public boolean remove(CacheTransactionManager manager, StreamCache cache,
    		String key, long time) throws StorageException{       
    	try{
    		if(this.managed.contains(key)){
    			return this.entities.put(key, EMPTY) != null;
    		}
    		else{
    			this.lock(manager, key, time);
    			this.managed.add(key);
    			this.updated.add(key);
    			this.entities.put(key, null);
    			return cache.getStream(key) != null;
    		}
    	}
    	catch(Throwable e){
    		throw new StorageException(e);
    	}
    }
	
    /* métodos de manipulação*/
    
	public void savePoint(StreamCache cache) throws IOException, RecoverException{
		saved.clear();

		for(String key: this.updated){
			InputStream in = cache.getStream(key);
			if(in != null){
				saved.put(key, new EntryCache(this.getBytes(in), -1));
			}
			else{
				saved.put(key, null);
			}
		}
		
	}
    
	public void rollback(StreamCache cache) throws StorageException, RecoverException {
		
		for(String key: this.saved.keySet()){
			EntryCache entity = saved.get(key);
			if(entity == null){
				cache.remove(key);
			}
			else{
				cache.putStream(key, entity.getMaxAlive(), new ByteArrayInputStream(entity.getData()));
			}
		}
		
	}
	
	public void commit(StreamCache cache) throws RecoverException, StorageException {
		if(!this.updated.isEmpty()){
			for(String key: this.updated){
				EntryCache entity = this.entities.get(key);
				
				if(entity == null){
					cache.remove(key);
				}
				else{
					cache.putStream(key, entity.getMaxAlive(), new ByteArrayInputStream(entity.getData()));
				}
			}
			
		}
	}
	
	public void clear() throws TransactionException{
		this.updated.clear();
		this.locked.clear();
		this.managed.clear();
		this.entities.clear();
		this.saved.clear();
	}
    
    /* métodos internos */
    
    private byte[] getEntity(CacheTransactionManager manager, StreamCache cache,
    		String key, boolean lock, long time) 
    		throws RecoverException, IOException, TransactionException{
    	
    	if(this.managed.contains(key)){
    		EntryCache entry = this.entities.get(key);
    		return entry.getData();
    	}
    	else{
    		byte[] dta = this.getSharedEntity(manager, cache, key, lock, time);
			this.managed.add(key);
			this.entities.put(key, EMPTY);
    		
    		return dta;
    	}
    }
    
    private void lock(CacheTransactionManager manager, String key, long time) throws TransactionException{
    	
    	if(this.locked.contains(key)){
    		return;
    	}
    	
    	if(time <= 0){
    		manager.lock(this.id, key);
    	}
    	else{
			manager.tryLock(this.id, key, time, TimeUnit.MILLISECONDS);
    	}
    	
    	this.locked.add(key);
    }
    
    private byte[] getSharedEntity(CacheTransactionManager manager, StreamCache cache,
    		String key, boolean lock, long time) 
    		throws IOException, TransactionException, RecoverException{
    	
		if(lock){
			this.lock(manager, key, time);
		}
		
		InputStream in = cache.getStream(key);
		
		if(in != null){
			byte[] dta = this.getBytes(in);
			return dta;
		}
		
		return in == null? null : this.getBytes(in);
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
