package org.brandao.brcache.tx;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.brandao.brcache.BRCacheConfig;
import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.CacheHandler;
import org.brandao.brcache.DataMap;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

public class TXCacheHandler implements CacheHandler{

	private static final long serialVersionUID = 3313232038342358036L;

    private static final Method putStream;

    private static final Method replaceStream;
    
    private static final Method putIfAbsentStream;
    
    private static final Method getStream;
    
    private static final Method removeStream;
    
    private static final Method containsKey;

    private static final Method getPointer;

    private static final Method setPointer;
    
    private static final Method replacePointer;
    
    private static final Method remove;
    
    private static final Method releaseSegments;
    
    private static final Method getStream_;
    
    private static final Method putData;

    private static final Method getNextModCount;
    
    private static final Method getConfig;
    
    private static final Method getCountRead;

    private static final Method getCountWrite;

    private static final Method getCountRemoved;

    private static final Method getCountReadData;
    
    private static final Method getCountWriteData;

    private static final Method getCountRemovedData;
    
    private static final Method isDeleteOnExit;

    private static final Method setDeleteOnExit;

    private static final Method size;
	
    private static final Method isEmpty;
	
    private static final Method clear;
	
    private static final Method destroy;

	
	static{
		try{
		    putStream = CacheTransactionHandler.class.getDeclaredMethod("putStream", CacheTransactionManager.class,
					CacheHandler.class, String.class, InputStream.class, 
		    		long.class, long.class);

		    replaceStream = CacheTransactionHandler.class.getDeclaredMethod("replaceStream", CacheTransactionManager.class,
					CacheHandler.class, String.class, InputStream.class, 
		    		long.class, long.class);
		    
		    putIfAbsentStream = CacheTransactionHandler.class.getDeclaredMethod("putIfAbsentStream", CacheTransactionManager.class,
					CacheHandler.class, String.class, InputStream.class, 
		    		long.class, long.class);
		    
		    getStream = CacheTransactionHandler.class.getDeclaredMethod("getStream", CacheTransactionManager.class,
					CacheHandler.class, String.class);
		    
		    removeStream = CacheTransactionHandler.class.getDeclaredMethod("removeStream", CacheTransactionManager.class,
					CacheHandler.class, String.class);
		    
		    containsKey = CacheTransactionHandler.class.getDeclaredMethod("containsKey", CacheTransactionManager.class,
					CacheHandler.class, String.class);

		    getPointer = CacheTransactionHandler.class.getDeclaredMethod("getPointer", CacheTransactionManager.class,
					CacheHandler.class, String.class);

		    setPointer = CacheTransactionHandler.class.getDeclaredMethod("setPointer", CacheTransactionManager.class,
					CacheHandler.class, String.class, DataMap.class);
		    
		    replacePointer = CacheTransactionHandler.class.getDeclaredMethod("replacePointer", CacheTransactionManager.class,
					CacheHandler.class, String.class, DataMap.class, DataMap.class);
		    
		    remove = CacheTransactionHandler.class.getDeclaredMethod("remove", CacheTransactionManager.class,
					CacheHandler.class, String.class, DataMap.class);
		    
		    releaseSegments = CacheTransactionHandler.class.getDeclaredMethod("releaseSegments", CacheTransactionManager.class,
					CacheHandler.class, DataMap.class);
		    
		    getStream_ = CacheTransactionHandler.class.getDeclaredMethod("getStream", CacheTransactionManager.class,
					CacheHandler.class, String.class, DataMap.class);
		    
		    putData = CacheTransactionHandler.class.getDeclaredMethod("putData", CacheTransactionManager.class,
					CacheHandler.class, DataMap.class, InputStream.class);

		    getNextModCount = CacheTransactionHandler.class.getDeclaredMethod("getNextModCount", CacheTransactionManager.class,
					CacheHandler.class);
		    
			getConfig = CacheTransactionHandler.class.getDeclaredMethod("getConfig", CacheTransactionManager.class,
					CacheHandler.class);
		    
		    getCountRead = CacheTransactionHandler.class.getDeclaredMethod("getCountRead", CacheTransactionManager.class,
					CacheHandler.class);

		    getCountWrite = CacheTransactionHandler.class.getDeclaredMethod("getCountWrite", CacheTransactionManager.class,
					CacheHandler.class);

		    getCountRemoved = CacheTransactionHandler.class.getDeclaredMethod("getCountRemoved", CacheTransactionManager.class,
					CacheHandler.class);

		    getCountReadData = CacheTransactionHandler.class.getDeclaredMethod("getCountReadData", CacheTransactionManager.class,
					CacheHandler.class);
		    
		    getCountWriteData = CacheTransactionHandler.class.getDeclaredMethod("getCountWriteData", CacheTransactionManager.class,
					CacheHandler.class);

		    getCountRemovedData = CacheTransactionHandler.class.getDeclaredMethod("getCountRemovedData", CacheTransactionManager.class,
					CacheHandler.class);
		    
		    isDeleteOnExit = CacheTransactionHandler.class.getDeclaredMethod("isDeleteOnExit", CacheTransactionManager.class,
					CacheHandler.class);

			setDeleteOnExit = CacheTransactionHandler.class.getDeclaredMethod("setDeleteOnExit", CacheTransactionManager.class,
					CacheHandler.class, boolean.class);

			size = CacheTransactionHandler.class.getDeclaredMethod("size", CacheTransactionManager.class,
					CacheHandler.class);
			
			isEmpty = CacheTransactionHandler.class.getDeclaredMethod("isEmpty", CacheTransactionManager.class,
					CacheHandler.class);
			
			clear = CacheTransactionHandler.class.getDeclaredMethod("clear", CacheTransactionManager.class,
					CacheHandler.class);
			
			destroy = CacheTransactionHandler.class.getDeclaredMethod("destroy", CacheTransactionManager.class,
					CacheHandler.class);

		}
		catch(Throwable e){
			throw new ExceptionInInitializerError(e);
		}
	}
	
	private CacheHandler cacheHandler;
	
	private CacheTransactionManager transactionManager;
	
	public TXCacheHandler(CacheHandler cacheHandler, CacheTransactionManager transactionManager) {
		this.cacheHandler = cacheHandler;
		this.transactionManager = transactionManager;
	}

	public boolean putStream(String key, InputStream inputData,
			long timeToLive, long timeToIdle) throws StorageException {
		try{
			return (Boolean)this.executeMethodInTX(putStream, 
					transactionManager.getCurrrent(false), 
					transactionManager, cacheHandler, 
		    		key, inputData, timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}

	public boolean replaceStream(String key, InputStream inputData,
			long timeToLive, long timeToIdle) throws StorageException {
		try{
			return (Boolean)this.executeMethodInTX(replaceStream, 
					transactionManager.getCurrrent(false), 
					transactionManager, cacheHandler,
					key, inputData, timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}

	public InputStream putIfAbsentStream(String key, InputStream inputData,
			long timeToLive, long timeToIdle) throws StorageException {
		try{
			return (InputStream)this.executeMethodInTX(putIfAbsentStream, 
					transactionManager.getCurrrent(false), 
					transactionManager, cacheHandler,
					key, inputData, timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}

	public InputStream getStream(String key) throws RecoverException {
		return getStream(key, false);
	}

	public InputStream getStream(String key, boolean forUpdate) throws RecoverException {
		try{
			return (InputStream)this.executeMethodInTX(getStream, 
					transactionManager.getCurrrent(false), 
					transactionManager, cacheHandler, 
		    		key, forUpdate);
		}
		catch(RecoverException e){
			throw e;
		}
		catch(CacheException e){
			throw new RecoverException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new RecoverException(e, CacheErrors.ERROR_1021);
		}
	}
	
	public boolean removeStream(String key) throws StorageException {
		try{
			return (Boolean)this.executeMethodInTX(remove, 
					transactionManager.getCurrrent(false), 
					transactionManager, cacheHandler,
		    		key);
		}
		catch(StorageException e){
			throw e;
		}
		catch(CacheException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}

	public boolean containsKey(String key) {
		return false;
	}

	public DataMap getPointer(String key) throws RecoverException {
		return null;
	}

	public void setPointer(String key, DataMap newDta) throws RecoverException {
	}

	public boolean replacePointer(String key, DataMap originalDta,
			DataMap newDta) throws RecoverException {
		return false;
	}

	public void remove(String key, DataMap data) {
	}

	public void releaseSegments(DataMap map) {
	}

	public InputStream getStream(String key, DataMap map)
			throws RecoverException {
		return null;
	}

	public void putData(DataMap map, InputStream inputData)
			throws StorageException, InterruptedException {
	}

	public long getNextModCount() {
		return cacheHandler.getNextModCount();
	}

	public BRCacheConfig getConfig() {
		return cacheHandler.getConfig();
	}

	public long getCountRead() {
		return cacheHandler.getCountRead();
	}

	public long getCountWrite() {
		return cacheHandler.getCountWrite();
	}

	public long getCountRemoved() {
		return cacheHandler.getCountRemoved();
	}

	public long getCountReadData() {
		return cacheHandler.getCountReadData();
	}

	public long getCountWriteData() {
		return cacheHandler.getCountWriteData();
	}

	public long getCountRemovedData() {
		return cacheHandler.getCountRemovedData();
	}

	public boolean isDeleteOnExit() {
		return cacheHandler.isDeleteOnExit();
	}

	public void setDeleteOnExit(boolean deleteOnExit) {
		throw new UnsupportedOperationException();
	}

	public long size() {
		return cacheHandler.size();
	}

	public boolean isEmpty() {
		return cacheHandler.isEmpty();
	}

	public void clear() {
		throw new UnsupportedOperationException();
	}

	public void destroy() {
	}

    private Object executeMethodInTX(Method method, 
    		CacheTransactionHandler currentTx, Object ... params) throws Throwable{
    	
    	CacheTransactionHandler tx = 
    			currentTx == null? 
    					transactionManager.begin(this) : 
    					currentTx;
    	
    	try{
    		Object r = method.invoke(tx, params);
			if(currentTx == null){
				tx.commit();
			}
			return r;
    	}
    	catch(IllegalAccessException e){
    		throw new CacheException(new Exception("bug!", e), CacheErrors.ERROR_1023);
    	}
    	catch(IllegalArgumentException e){
    		throw new CacheException(new Exception("bug!", e), CacheErrors.ERROR_1023);
    	}
    	catch(InvocationTargetException e){
    		Throwable ex = e.getTargetException();
    		
    		try{
    			if(currentTx == null){
    				tx.rollback();
    			}
    		}
			catch(Throwable x){
	    		throw new CacheException(
    				new Exception(
						"bug: exception not recognized (rollback fail): " + x.toString(), ex), 
						CacheErrors.ERROR_1018);
				
			}
    		
    		if(ex instanceof StorageException || ex instanceof RecoverException){
        		throw ex;
    		}
    		else{
        		throw new CacheException(new Exception("bug: exception not recognized: ", e), CacheErrors.ERROR_1023);
    		}
    		
    	}

    }	
}
