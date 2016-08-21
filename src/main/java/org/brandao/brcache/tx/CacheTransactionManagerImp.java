package org.brandao.brcache.tx;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.StreamCache;
import org.brandao.concurrent.NamedLock;

public class CacheTransactionManagerImp 
	implements CacheTransactionManager{

	private ThreadLocal<CacheTransactionHandler> transactions;
	
	private NamedLock locks;
	
	private Map<UUID, Transaction> transactionLocks;
	
	private String transactionPath;
	
	public CacheTransactionManagerImp(){
		this.transactionLocks = new HashMap<UUID, CacheTransactionManagerImp.Transaction>();
		this.locks = new NamedLock();
		this.transactionPath = "/mnt/brcache/tx";
		this.transactions = new ThreadLocal<CacheTransactionHandler>();
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
			throw new TransactionException(e);
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
					throw new TransactionException("timeout");
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
					new TransactionException(e);
		}
	}

	public void unlock(UUID txId, String key)
			throws TransactionException {
		Transaction tx = transactionLocks.get(txId);
		
		if(tx == null){
			throw new TransactionException("tx not found: " + txId);
		}
		
		Serializable lockId = tx.locks.get(key);
		
		if(lockId == null){
			throw new TransactionException("lock not found: " + txId + ":" + key);
		}
		
		try{
			locks.unlock(lockId, key);
			tx.locks.remove(key);
		}
		catch(Throwable e){
			if(lockId != null){
				this.locks.unlock(lockId, key);
			}
			throw new TransactionException(e);
		}
	}

	public void commit(UUID txId) throws TransactionException {
		Transaction tx = this.transactionLocks.get(txId);
		
		if(tx == null){
			throw new TransactionException("tx not found: " + txId);
		}
		
		CacheTransactionHandler txHandler = tx.txHandler;
		
		txHandler.commit();
	}

	public void rollback(UUID txId) throws TransactionException {
		Transaction tx = this.transactionLocks.get(txId);
		
		if(tx == null){
			throw new TransactionException("tx not found: " + txId);
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
			throw new IllegalStateException("transaction has been started");
		}
		
		UUID txId = UUID.randomUUID();
		txh = new CacheTransactionHandlerImp(txId, this, cache);
		this.transactionLocks.put(txId, new Transaction(txh, new HashMap<String, Serializable>()));
		this.transactions.set(txh);
		txh.begin();
		return txh;
	}

	public CacheTransactionHandler getCurrrent() {
		return this.transactions.get();
	}

	public void close(CacheTransactionHandler tx) throws TransactionException {
		
		CacheTransactionHandler current = this.transactions.get();
		
		if(current != tx){
			throw new TransactionException("invalid current transaction");
		}
		
		UUID txId = (UUID) tx.getId();
		
		Transaction txInfo = this.transactionLocks.remove(txId);
		Map<String, Serializable> locks = txInfo.locks;
		
		for(String lockName: locks.keySet()){
			Serializable ref = locks.get(lockName);
			this.locks.unlock(ref, lockName);
		}
		
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