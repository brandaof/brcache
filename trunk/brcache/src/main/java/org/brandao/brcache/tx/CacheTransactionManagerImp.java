package org.brandao.brcache.tx;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.BRCacheConfig;
import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.StreamCache;
import org.brandao.concurrent.NamedLock;

public class CacheTransactionManagerImp 
	implements CacheTransactionManager{

	private ThreadLocal<CacheTransactionHandler> transactions;
	
	private NamedLock locks;
	
	private Map<UUID, Transaction> transactionLocks;
	
	private String transactionPath;
	
	private BRCacheConfig config;

	private BRCacheTransactionConfig cacheTransactionConfig;
    
	public CacheTransactionManagerImp(){
	}
	
	public void setConfiguration(BRCacheConfig config){
		this.config                 = config;
		this.transactionLocks       = new HashMap<UUID, CacheTransactionManagerImp.Transaction>();
		this.locks                  = new NamedLock();
		this.transactions           = new ThreadLocal<CacheTransactionHandler>();
		
		this.cacheTransactionConfig = new BRCacheTransactionConfig();
		this.cacheTransactionConfig.setConfiguration(config.getConfiguration());
		
        this.transactionPath = this.cacheTransactionConfig.getDataPath();

	}
	
	public void lock(UUID txId, String key) throws TransactionException {
		Serializable lockId = null;
		try{
			Transaction tx = transactionLocks.get(txId);
			if(!tx.locks.containsKey(key)){
				lockId = locks.lock(key);
				tx.locks.put(key, lockId);
			}
		}
		catch(Throwable e){
			if(lockId != null){
				this.locks.unlock(lockId, key);
			}
			throw new TransactionException(e, CacheErrors.ERROR_1023);
		}
	}

	public void tryLock(UUID txId, String key, long time, TimeUnit unit)
			throws TransactionException {
		Serializable lockId = null;
		try{
			Transaction tx = transactionLocks.get(txId);
			
			if(!tx.locks.containsKey(key)){
				lockId = locks.tryLock(key, time, unit);
				
				if(lockId == null){
					throw new TransactionException(CacheErrors.ERROR_1024);
				}
				
				tx.locks.put(key, lockId);
			}
		}
		catch(Throwable e){
			if(lockId != null){
				this.locks.unlock(lockId, key);
			}
			throw e instanceof TransactionException? 
					(TransactionException)e : 
					new TransactionException(e, CacheErrors.ERROR_1023);
		}
	}

	public void unlock(UUID txId, String key)
			throws TransactionException {
		Transaction tx = transactionLocks.get(txId);
		
		if(tx == null){
			throw new TransactionException(CacheErrors.ERROR_1025, txId);
		}
		
		Serializable lockId = tx.locks.get(key);
		
		if(lockId == null){
			throw new TransactionException(CacheErrors.ERROR_1026, txId, key);
		}
		
		try{
			locks.unlock(lockId, key);
			tx.locks.remove(key);
		}
		catch(Throwable e){
			if(lockId != null){
				this.locks.unlock(lockId, key);
			}
			throw e instanceof TransactionException? 
					(TransactionException)e : 
					new TransactionException(e, CacheErrors.ERROR_1023);
		}
	}

	public void commit(UUID txId) throws TransactionException {
		Transaction tx = this.transactionLocks.get(txId);
		
		if(tx == null){
			throw new TransactionException(CacheErrors.ERROR_1025, txId);
		}
		
		CacheTransactionHandler txHandler = tx.txHandler;
		
		txHandler.commit();
	}

	public void rollback(UUID txId) throws TransactionException {
		Transaction tx = this.transactionLocks.get(txId);
		
		if(tx == null){
			throw new TransactionException(CacheErrors.ERROR_1025, txId);
		}
		
		CacheTransactionHandler txHandler = tx.txHandler;
		
		txHandler.rollback();
	}

	
	public void setTransactionPath(String transactionPath) {
		this.transactionPath = transactionPath;
	}

	public String getTransactionPath() {
		return this.transactionPath;
	}

	public CacheTransactionHandler begin(StreamCache cache) {
		CacheTransactionHandler txh = this.transactions.get();
		
		if(txh != null){
			throw new TransactionException(CacheErrors.ERROR_1016);
		}
		
		UUID txId = UUID.randomUUID();
		//txh = new CacheTransactionHandlerImp(txId, this, cache);
		
		txh = new CacheTransactionHandlerImp(
				this.cacheTransactionConfig, txId, this, cache);
		
		this.transactionLocks.put(txId, new Transaction(txh, new HashMap<String, Serializable>()));
		this.transactions.set(txh);
		txh.begin();
		return txh;
	}

	public CacheTransactionHandler getCurrrent() {
		return this.getCurrrent(true);
	}
	
	public CacheTransactionHandler getCurrrent(boolean required) {
		CacheTransactionHandler currentTX = this.transactions.get();
		
		if(currentTX == null && required){
			throw new TransactionException(CacheErrors.ERROR_1013);
		}
		else{
			return currentTX;
		}
	}

	public void close(CacheTransactionHandler tx) throws TransactionException {
		
		CacheTransactionHandler current = this.transactions.get();
		
		if(current != tx){
			throw new TransactionException(CacheErrors.ERROR_1027);
		}
		
		UUID txId = (UUID) tx.getId();
		
		Transaction txInfo = this.transactionLocks.remove(txId);
		Map<String, Serializable> locks = txInfo.locks;
		
		for(String lockName: locks.keySet()){
			Serializable ref = locks.get(lockName);
			this.locks.unlock(ref, lockName);
		}
		
		txInfo.txHandler.close();
		this.transactions.remove();
	}

	private class Transaction{
		
		public CacheTransactionHandler txHandler;
		
		public Map<String, Serializable> locks;

		public Transaction(CacheTransactionHandler txHandler,
				Map<String, Serializable> locks) {
			this.txHandler = txHandler;
			this.locks = locks;
		}
		
	}
}
