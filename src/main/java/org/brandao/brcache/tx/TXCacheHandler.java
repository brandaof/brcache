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

	private static final Method replaceStream;
	
	private static final Method putIfAbsentStream;
	
	private static final Method putStream;

	private static final Method getStream;

	private static final Method remove;

	static{
		try{
			replaceStream = CacheTransactionHandler.class.getDeclaredMethod(
					"replaceStream", CacheTransactionManager.class, 
					CacheHandler.class, String.class, InputStream.class, long.class, long.class);
			
			putIfAbsentStream = CacheTransactionHandler.class.getDeclaredMethod(
					"putIfAbsentStream", CacheTransactionManager.class, CacheHandler.class,
					String.class, InputStream.class, long.class, long.class);
			
			putStream = CacheTransactionHandler.class.getDeclaredMethod(
					"putStream", CacheTransactionManager.class, CacheHandler.class, 
		    		String.class, InputStream.class, long.class, long.class);

			getStream = CacheTransactionHandler.class.getDeclaredMethod(
					"getStream", CacheTransactionManager.class, CacheHandler.class, 
		    		String.class, boolean.class);

			remove = CacheTransactionHandler.class.getDeclaredMethod(
					"remove", CacheTransactionManager.class, CacheHandler.class,
		    		String.class);

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
		// TODO Auto-generated method stub
		return false;
	}

	public DataMap getPointer(String key) throws RecoverException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setPointer(String key, DataMap newDta) throws RecoverException {
		// TODO Auto-generated method stub
		
	}

	public boolean replacePointer(String key, DataMap originalDta,
			DataMap newDta) throws RecoverException {
		// TODO Auto-generated method stub
		return false;
	}

	public void remove(String key, DataMap data) {
		// TODO Auto-generated method stub
		
	}

	public void releaseSegments(DataMap map) {
		// TODO Auto-generated method stub
		
	}

	public InputStream getStream(String key, DataMap map)
			throws RecoverException {
		// TODO Auto-generated method stub
		return null;
	}

	public void putData(DataMap map, InputStream inputData)
			throws StorageException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	public long getNextModCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public BRCacheConfig getConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	public long getCountRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountWrite() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountRemoved() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountReadData() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountWriteData() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getCountRemovedData() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isDeleteOnExit() {
		// TODO Auto-generated method stub
		return false;
	}

	public void setDeleteOnExit(boolean deleteOnExit) {
		// TODO Auto-generated method stub
		
	}

	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public void clear() {
		// TODO Auto-generated method stub
		
	}

	public void destroy() {
		// TODO Auto-generated method stub
		
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
