package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.TXCache;
import org.brandao.brcache.tx.TXCacheImp;

/**
 * Provê as operações básicas de um cache.
 * 
 * @author Brandao.
 *
 */
public class BasicCache extends AbstractCache {

	private static final long serialVersionUID = -3101078990483084845L;
	
    public BasicCache(BRCacheConfig config){
    	super("default", config);
    }

    /**
     * Cria um novo cache.
     * 
     * @param cacheHandler Manpulador do cache.
     */
    public BasicCache(CacheHandler cacheHandler){
    	super(cacheHandler);
    }
    
    /**
     * Obtém o cache com suporte transacional com um gestor transacional específico.
     * @param txManager gestor transacional.
     * @return cache com suporte transacional.
     */
    public TXCache getTXCache(CacheTransactionManager txManager){
    	return new TXCacheImp(super.cacheHandler, txManager);
    }
    
	/* métodos de armazenamento */
    
	public boolean replace(String key, Object value, 
			long timeToLive, long timeToIdle) throws StorageException {
		
		ByteArrayOutputStream bout;
		
		try{
			bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
		
		try{
			return super.replaceStream(key, new ByteArrayInputStream(bout.toByteArray()), 
					timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(RecoverException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
	
    public boolean replaceStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return super.replaceStream(key, inputData, timeToLive, timeToIdle);
    }
	
    public Object putIfAbsent(String key, Object value, 
    		long timeToLive, long timeToIdle) throws StorageException{
		ByteArrayOutputStream bout;
		
		try{
			bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}

		try{
			InputStream in =
				super.putIfAbsentStream(key, new ByteArrayInputStream(bout.toByteArray()),
						timeToLive, timeToIdle);
			
			if(in != null){
				ObjectInputStream oin = new ObjectInputStream(in);
				return oin.readObject();
			}
			else
				return null;
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
    }
    
    public InputStream putIfAbsentStream(String key, InputStream inputData,
    		long timeToLive, long timeToIdle) throws StorageException{
    	return super.putIfAbsentStream(key, inputData, timeToLive, timeToIdle);
    }
    
	public boolean put(String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		ByteArrayOutputStream bout;
		
		try{
			bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
		
		try{
			return this.putStream(key, new ByteArrayInputStream(bout.toByteArray()), 
					timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}
    
    public boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
    	return super.putStream(key, inputData, timeToLive, timeToIdle);
    }
	
    /* métodos de coleta */
    
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
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
	}
	
    public InputStream getStream(String key) throws RecoverException {
    	return super.getStream(key);
    }
    
    /* métodos de remoção */
    
	public BRCacheConfig getConfig() {
		return cacheHandler.getConfig();
	}

	public boolean replace(String key, Object oldValue, Object newValue,
			long timeToLive, long timeToIdle) throws StorageException {
		throw new UnsupportedOperationException();
	}

	public boolean remove(String key, Object value) throws StorageException {
		throw new UnsupportedOperationException();
	}

	public long size() {
		return super.getCountRemoved() - super.getCountWrite();
	}

	public boolean isEmpty() {
		return this.size() == 0;
	}
    
}
