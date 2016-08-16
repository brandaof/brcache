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
		try{
			lockId = locks.tryLock(key, time, unit);
			tx.locks.add(lockId);
		}
		catch(Throwable e){
			if(lockId != null){
				this.locks.unlock(lockId, key);
			}
			throw new TransactionException(e);
		}
	}

	public void commit(UUID transaction) throws TransactionException {
		// TODO Auto-generated method stub
		
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
		
		public Map<String, Serializable> locks;
		
	}
}
