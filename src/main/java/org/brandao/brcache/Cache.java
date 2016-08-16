package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Cache 
	extends StreamCache {

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
    }
    
	public long size() {
		return super.getCountRemoved() - super.getCountWrite();
	}

	public boolean isEmpty() {
		return super.getCountWrite() == 0;
	}

	public boolean containsKey(String key) throws RecoverException {
		return super.getStream(key) != null;
	}

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

	public boolean remove(String key) throws RecoverException {
		return super.remove(key);
	}

	public synchronized Object putIfAbsent(String key, 
			Object value, long maxAliveTime) throws StorageException {
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
	}

	public boolean remove(String key, Object value) throws StorageException {
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
	}

	public boolean replace(String key, Object oldValue, 
			Object newValue, long maxAliveTime) throws StorageException {
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
	}

	public Object replace(String key, Object value, long maxAliveTime) throws StorageException {
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
	}

}
