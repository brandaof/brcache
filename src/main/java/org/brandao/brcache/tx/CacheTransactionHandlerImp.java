package org.brandao.brcache.tx;

import java.io.InputStream;
import java.io.Serializable;

import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.CacheHandler;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

class CacheTransactionHandlerImp
	implements CacheTransactionHandler{

	private TransactionInfo transactionInfo;
	
	private boolean commitInProgress;
	
	private boolean started;

	private boolean rolledBack;
	
	private boolean commited;
	
	private CacheHandler cache;
	
	private CacheTransactionManager transactionManager;
	
	private Serializable id;
	
	private long timeout;
	
	public CacheTransactionHandlerImp(
			Serializable id, 
			CacheTransactionManager transactionManager, 
			CacheHandler cache, long timeout){
		
		this.commitInProgress   = false;
		this.started            = false;
		this.commited           = false;
		this.rolledBack         = false;
		this.cache				= cache;
		this.transactionManager	= transactionManager;
		this.id					= id;
		this.timeout            = timeout;
	}
	
	public Serializable getId() {
		return id;
	}
	
	public synchronized void begin() {
		
		transactionInfo = new TransactionInfo(id, timeout);
		
		if(started){
			throw new TransactionException(CacheErrors.ERROR_1016);
		}

		if(commitInProgress){
			throw new TransactionException(CacheErrors.ERROR_1010);
		}
		
		started = true;
	}
	
	public boolean isRolledBack() {
		return rolledBack;
	}

	public boolean isCommited() {
		return commited;
	}

	public void rollback() throws TransactionException {
		this.rollback(transactionInfo);
	}
	
	private void rollback(TransactionInfo transactionInfo) throws TransactionException {

		if(rolledBack){
			throw new TransactionException(CacheErrors.ERROR_1011);
		}

		if(commited){
			throw new TransactionException(CacheErrors.ERROR_1012);
		}
		
		if(!started){
			throw new TransactionException(CacheErrors.ERROR_1013);
		}
		
		try{
			if(commitInProgress){
				transactionInfo.rollback(cache);
				closeTransaction(transactionInfo);
			}
			else{
				closeTransaction(transactionInfo);
				return;
			}
			this.rolledBack = true;
			this.commited   = false;
		}
		catch(TransactionException e){
			throw e;
		}
		catch(CacheException e){
			throw new TransactionException(e, e.getError(), e.getParams());
		}
		catch (Throwable e) {
			throw new TransactionException(e, CacheErrors.ERROR_1018);
		}
		
	}
	
	protected void closeTransaction(TransactionInfo transactionInfo) throws TransactionException{
		transactionManager.close(this);
	}
	
	public void close(){
		try{
			transactionInfo.close(cache);
			cache              = null;
			transactionInfo    = null;
			transactionManager = null;
			started            = false;
			commitInProgress   = false;
		}
		catch(Throwable e){
			throw new TransactionException(e, CacheErrors.ERROR_1023);
		}
	}
	
	public void commit() throws TransactionException {
		if(this.rolledBack){
			throw new TransactionException(CacheErrors.ERROR_1011);
		}

		if(this.commited){
			throw new TransactionException(CacheErrors.ERROR_1012);
		}
		
		if(!started){
			throw new TransactionException(CacheErrors.ERROR_1013);
		}
		
		try{
			this.commitInProgress = true;
			this.transactionInfo.commit(cache);
			this.closeTransaction(this.transactionInfo);
			this.commitInProgress = false;
			this.commited         = true;
			this.rolledBack       = false;
		}
		catch(TransactionException e){
			throw e;
		}
		catch(CacheException e){
			throw new TransactionException(e, e.getError(), e.getParams());
		}
		catch(Throwable e){
			throw new TransactionException(e, CacheErrors.ERROR_1019);
		}
		
	}

	/* métodos do TXCacheHandler */
	
	public boolean replaceStream(CacheTransactionManager manager,
			CacheHandler cache, String key, InputStream inputData,
			long timeToLive, long timeToIdle)
			throws StorageException {
		return transactionInfo.replaceStream(manager, cache, key, inputData, 
				timeToLive, timeToIdle);
	}

	public InputStream putIfAbsentStream(CacheTransactionManager manager,
			CacheHandler cache, String key, InputStream inputData,
			long timeToLive, long timeToIdle)
			throws StorageException {
		return transactionInfo.putIfAbsentStream(manager, cache, key, inputData, timeToLive, timeToIdle);
	}

	public boolean putStream(CacheTransactionManager manager, CacheHandler cache,
			String key,InputStream inputData, long timeToLive, long timeToIdle) throws StorageException {
		return transactionInfo.putStream(manager, cache, key, inputData, timeToLive, timeToIdle);
	}

	public InputStream getStream(CacheTransactionManager manager,
			CacheHandler cache, String key, boolean forUpdate)
			throws RecoverException {
		return transactionInfo.getStream(manager, cache, key, forUpdate);
	}

	public boolean remove(CacheTransactionManager manager, CacheHandler cache,
			String key) throws StorageException {
		return transactionInfo.remove(manager, cache, key);
	}

}
