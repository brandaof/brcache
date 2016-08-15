package org.brandao.brcache.tx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.brandao.brcache.NonTransactionalCache;
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
	
	public void putObject(CacheTransactionManager manager, NonTransactionalCache cache,
			String key, long maxAliveTime, Object item)
			throws StorageException {
		
		try{
	        ByteArrayOutputStream bout = new ByteArrayOutputStream();
	        ObjectOutputStream oout = new ObjectOutputStream(bout);
	        oout.writeObject(item);
	        oout.flush();
	        this.put(manager, cache, key, maxAliveTime, new ByteArrayInputStream(bout.toByteArray()));
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}

	public Object getObject(CacheTransactionManager manager, NonTransactionalCache cache,
			String key, boolean forUpdate, long time)
			throws RecoverException {
    	InputStream in = null;
        try{
            in = this.get(manager, cache, key, forUpdate, time);
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
        finally{
        	try{
        		if(in != null)
        			in.close();
        	}
        	catch(Throwable e){
        	}
        }
	}
	
    public void put(CacheTransactionManager manager, NonTransactionalCache cache, 
    		String key, long maxAliveTime, InputStream inputData) 
    		throws StorageException {
    	
    	try{
			manager.lock(this.id, key);
			
			byte[] dta = inputData == null? null : this.getBytes(inputData);
			
			this.managed.add(key);
			this.inserted.add(key);
			this.entities.put(key, new EntryCache(dta, maxAliveTime));
    	}
    	catch(Throwable e){
    		throw new StorageException(e);
    	}
    }

    public InputStream get(CacheTransactionManager manager, NonTransactionalCache cache, 
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

    public boolean remove(CacheTransactionManager manager, NonTransactionalCache cache,
    		String key) throws RecoverException{       
    	try{
    		manager.lock(this.id, key);
			this.managed.add(key);
			this.inserted.add(key);
			this.entities.put(key, null);
			return cache.get(key) != null;
    	}
    	catch(RecoverException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new RecoverException(e);
    	}
    }
    
    private byte[] getEntity(CacheTransactionManager manager, NonTransactionalCache cache,
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
    
    private byte[] getSharedEntity(CacheTransactionManager manager, NonTransactionalCache cache,
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

	public void savePoint(NonTransactionalCache cache) throws IOException, RecoverException{
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
    
	public void rollback(NonTransactionalCache cache) throws StorageException, RecoverException {
		
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
	
	protected void commit(NonTransactionalCache cache) throws RecoverException, StorageException {
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
	
	public void clear() throws TransactionException{
		this.entities.clear();
		this.inserted.clear();
		this.saved.clear();
	}
	
}
