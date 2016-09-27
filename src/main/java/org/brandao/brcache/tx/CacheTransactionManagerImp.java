package org.brandao.brcache.tx;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.brandao.brcache.BasicCache;
import org.brandao.brcache.CacheErrors;
import org.brandao.concurrent.NamedLock;

/**
 * Implementação padrão do gestor de transações.
 * @author Brandao
 *
 */
public class CacheTransactionManagerImp 
	implements CacheTransactionManager{

	private ThreadLocal<CacheTransactionHandler> transactions;
	
	private NamedLock locks;
	
	private ConcurrentMap<Serializable, Transaction> transactionLocks;
	
	private String transactionPath;
	
	private long timeout;
	
	public CacheTransactionManagerImp(String transactionPath, long timeout){
		this.transactionLocks = new ConcurrentHashMap<Serializable, CacheTransactionManagerImp.Transaction>();
		this.locks            = new NamedLock();
		this.transactions     = new ThreadLocal<CacheTransactionHandler>();
		this.transactionPath  = transactionPath;
		this.timeout          = timeout;
	}
	
	public void lock(Serializable txId, String key) throws TransactionException {
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

	public void tryLock(Serializable txId, String key, long time, TimeUnit unit)
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

	public void unlock(Serializable txId, String key)
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

	public void commit(Serializable txId) throws TransactionException {
		Transaction tx = this.transactionLocks.get(txId);
		
		if(tx == null){
			throw new TransactionException(CacheErrors.ERROR_1025, txId);
		}
		
		CacheTransactionHandler txHandler = tx.txHandler;
		
		txHandler.commit();
	}

	public void rollback(Serializable txId) throws TransactionException {
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

	public String getPath() {
		return this.transactionPath;
	}

	public long getTimeout() {
		return this.timeout;
	}

	public void setTimeout(long value) {
		this.timeout = value;
	}
	
	public CacheTransactionHandler begin(BasicCache cache) {
		CacheTransactionHandler txh = this.transactions.get();
		
		if(txh != null){
			throw new TransactionException(CacheErrors.ERROR_1016);
		}
		
		Serializable txId = this.createTransactionID();
		
		txh = new CacheTransactionHandlerImp(txId, this, cache, this.timeout);
		
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
		
		Serializable txId = tx.getId();
		
		Transaction txInfo = this.transactionLocks.remove(txId);
		Map<String, Serializable> locks = txInfo.locks;
		
		for(String lockName: locks.keySet()){
			Serializable ref = locks.get(lockName);
			this.locks.unlock(ref, lockName);
		}
		
		txInfo.txHandler.close();
		this.transactions.remove();
	}

	private AtomicInteger transactionIDs = new AtomicInteger(0);
	
	private Serializable createTransactionID(){
		Integer i = transactionIDs.incrementAndGet();
		return Integer.toString(i, Character.MAX_RADIX);
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
