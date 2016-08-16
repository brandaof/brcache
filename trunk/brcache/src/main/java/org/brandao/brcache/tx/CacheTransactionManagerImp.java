package org.brandao.brcache.tx;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.NamedLock;

public class CacheTransactionManagerImp 
	implements CacheTransactionManager{

	private NamedLock locks;
	
	private Map<UUID, Transaction> transactionLocks;
	
	public void lock(UUID txId, String key) throws TransactionException {
		Serializable lockId = null;
		try{
			lockId = locks.lock(key);
			Transaction tx = transactionLocks.get(txId);
			tx.locks.put(key, lockId);
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
			lockId = locks.tryLock(key, time, unit);
			
			if(lockId == null){
				throw new TransactionException("timeout");
			}
			
			Transaction tx = transactionLocks.get(txId);
			tx.locks.put(key, lockId);
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

	public void rollback(UUID transaction) throws TransactionException {
		// TODO Auto-generated method stub
		
	}

	public String getTransactionPath() {
		// TODO Auto-generated method stub
		return null;
	}

	public CacheTransactionHandler begin() {
		// TODO Auto-generated method stub
		return null;
	}

	public CacheTransactionHandler getCurrrent() {
		// TODO Auto-generated method stub
		return null;
	}

	public void close(CacheTransactionHandler tx) throws TransactionException {
		// TODO Auto-generated method stub
		
	}

	private class Transaction{
		
		public Serializable id;
		
		public CacheTransactionHandler txHandler;
		
		public Map<String, Serializable> locks;
		
	}
}
