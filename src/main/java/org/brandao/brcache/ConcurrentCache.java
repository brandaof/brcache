package org.brandao.brcache;

import java.io.InputStream;
import java.io.Serializable;

import org.brandao.concurrent.NamedLock;

/**
 * Provê as operações de um cache com bloqueio dos métodos que alteram o cache.
 * 
 * <pre>
 * ex:
 *    
 *    Cache cache = ...;
 *    cache.put("uma_chave", meuObjeto, 1200, 0);
 *    
 * ex2:
 *    
 *    MeuObjeto o = cache.get("uma_chave");
 *
 * </pre>
 * @author Brandao
 *
 */
public class ConcurrentCache extends BasicCache {
	
	private static final long serialVersionUID = -8558471389768293591L;

	protected transient NamedLock locks;
	
    /**
     * Cria um novo cache a partir de uma configuração específica.
     * @param config configuração.
     */
    public ConcurrentCache(BRCacheConfig config){
    	super(config);
    	this.locks = new NamedLock();
    }
    
    /**
     * Cria um novo cache.
     * 
     * @param cacheHandler Manpulador do cache.
     */
    public ConcurrentCache(CacheHandler cacheHandler){
    	super(cacheHandler);
    	this.locks = new NamedLock();
    }
    
	/* métodos de armazenamento */
	
	public boolean replace(String key, Object value, 
			long timeToLive, long timeToIdle) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			return super.replace(key, value, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
    public boolean replaceStream(String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
		Serializable refLock = this.locks.lock(key);
		try{
			return super.replaceStream(key, inputData, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
    }
	
	public boolean replace(String key, Object oldValue, 
			Object newValue, long timeToLive, long timeToIdle) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = super.get(key);
			if(o != null && o.equals(oldValue)){
				super.put(key, newValue, timeToLive, timeToIdle);
				return true;
			}
			else
				return false;
		}
		catch(StorageException e){
			throw e;
		}
		catch(RecoverException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	public Object putIfAbsent(String key,
			Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			return super.putIfAbsent(key, value, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
    public InputStream putIfAbsentStream(String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
    	
		Serializable refLock = this.locks.lock(key);
		try{
			return super.putIfAbsentStream(key, inputData, timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(RecoverException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
    }
	
	public boolean put(String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			return super.put(key, value, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
    public boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
		Serializable refLock = this.locks.lock(key);
		try{
			return super.putStream(key, inputData, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
    }
	
    /* métodos de coleta */
	
	public Object get(String key) throws RecoverException {
		return super.get(key);
	}

    public InputStream getStream(String key) throws RecoverException {
		return super.getStream(key);
    }
	
    /* métodos de remoção */

	public boolean remove(String key, Object value) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = super.get(key);
			if(o != null && o.equals(value)){
				return super.remove(key);
			}
			else
				return false;
		}
    	catch(StorageException e){
    		throw e;
    	}
    	catch(RecoverException e){
    		throw new StorageException(e, e.getError(), e.getParams());
    	}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
    public boolean remove(String key) throws StorageException{
		Serializable refLock = this.locks.lock(key);
		try{
			return super.remove(key);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
    }
	
}
