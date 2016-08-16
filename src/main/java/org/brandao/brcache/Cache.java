package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Cache 
	extends StreamCache {

	private static final long serialVersionUID = -8558471389768293591L;
	
	protected transient NamedLock locks;
	
    public Cache(){
        super(
    		3L*1024L*1024L, 1024, 0.5, 
    		1L*1024L*1024L, 1024, 0.5, 
    		10L*1024L*1024L, 64*1024, 1*1024, 0.5, 
    		1*1024*1024L, 100, "/mnt/brcache", SwaperStrategy.FILE, 1);
    }
    
    public Cache(
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
    	super(nodeBufferSize, nodePageSize, nodeSwapFactor, indexBufferSize, 
    			indexPageSize, indexSwapFactor, dataBufferSize, dataPageSize, blockSize, 
    			dataSwapFactor, maxSizeEntry, maxSizeKey, dataPath, swaperType, quantitySwaperThread);
    	this.locks = new NamedLock();
    }
    
	/* métodos de armazenamento */
	
	public Object replace(String key, Object value, 
			long maxAliveTime) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = this.get(key);
			if(o != null){
				this.put(key, value, maxAliveTime);
				return true;
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	public boolean replace(String key, Object oldValue, 
			Object newValue, long maxAliveTime) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = this.get(key);
			if(o != null && o.equals(oldValue)){
				this.put(key, newValue, maxAliveTime);
				return true;
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	public Object putIfAbsent(String key, 
			Object value, long maxAliveTime) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = this.get(key);
			if(o == null){
				this.put(key, value, maxAliveTime);
			}
			
			return o;
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	public void put(String key, Object value, long maxAliveTime) throws StorageException {
		try{
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
			this.put(key, maxAliveTime, new ByteArrayInputStream(bout.toByteArray()));
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}

	/* métodos de coleta*/
	
	public Object get(String key) throws RecoverException {
		try{
			InputStream in = super.getStream(key);
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

    /* métodos de remoção */

	public boolean remove(String key, Object value) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = this.get(key);
			if(o != null && o.equals(value)){
				return this.remove(key);
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	public boolean remove(String key) throws RecoverException {
		return super.remove(key);
	}

    /* métodos de manipulação*/
    
	public long size() {
		return super.getCountRemoved() - super.getCountWrite();
	}

	public boolean isEmpty() {
		return super.getCountWrite() == 0;
	}

	public boolean containsKey(String key) throws RecoverException {
		return super.getStream(key) != null;
	}
	
}
