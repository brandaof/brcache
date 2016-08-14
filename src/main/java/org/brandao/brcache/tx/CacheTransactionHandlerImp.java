package org.brandao.brcache.tx;

import java.io.File;
import java.io.InputStream;
import java.util.Set;

import org.brandao.brcache.Cache;

public class CacheTransactionHandlerImp 
	implements CacheTransactionHandler{

	private TransactionInfo transactionInfo;
	
	private boolean commitInProgress;
	
	private boolean started;

	private boolean rolledBack;
	
	private boolean commited;
	
	private Cache cache;
	
	private CacheTransactionManager transactionManager;
	
	private File file;

	private String transactionName;
	
	private CacheTransactionHandlerImp(byte[] id, 
			CacheTransactionManager transactionManager, Cache cache){
		this.commitInProgress   = false;
		this.started            = false;
		this.commited           = false;
		this.rolledBack         = false;
		this.transactionInfo    = new TransactionInfo(id);
		this.cache				= cache;
		this.transactionManager	= transactionManager;
		this.file               = new File(transactionManager.getTransactionPath(), transactionId);
		
	}
	
	public synchronized void begin() throws TransactionException {
		
		if(this.started)
			throw new TransactionException("transaction has been started");

		if(this.commitInProgress)
			throw new TransactionException("commit in progress");
		
		this.started = true;
	}
	
	public boolean isRolledBack() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isCommited() {
		// TODO Auto-generated method stub
		return false;
	}

	public void rollback() throws TransactionException {

		if(this.rolledBack)
			throw new TransactionException("transaction has been rolled back");

		if(this.commited)
			throw new TransactionException("transaction has been commited");
		
		if(!started)
			throw new TransactionException("transaction not started");
			
		try{
			if(this.commitInProgress){
				this.transactionInfo.rollback(this.cache);
				this.clearTransaction();
			}
			else{
				this.clearTransaction();
				return;
			}
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		
	}
	
	protected void clearTransaction() throws TransactionException{
		this.transactionManager.close(this);
		this.started = false;
		this.commitInProgress = false;
	}
	
	
	public void commit() throws TransactionException {
		if(this.rolledBack)
			throw new TransactionException("transaction has been rolled back");

		if(this.commited)
			throw new TransactionException("transaction has been commited");
		
		if(!started)
			throw new TransactionException("transaction not started");
		
		try{
			this.transactionInfo.savePoint(this.cache);
			
			this.persistTransaction(file, localTransactionInfo);
			try{
				this.lock(localTransactionInfo);
				
				for(String entityFileName: entities){
					EntityFile entityFile = this.manager.getEntityFile(entityFileName);
					TransactionInfo tx = localTransactionInfo.get(entityFileName);
					this.commit(tx, entityFile);
				}
			}
			finally{
				this.unlock(localTransactionInfo);
			}
			
			this.clearTransaction(this.file, localTransactionInfo);
			this.commited = true;
		}
		catch (Throwable e) {
			throw new TransactionException(e);
		}
	}

	public void putObject(CacheTransactionManager manager, Cache cache,
			String key, long maxAliveTime, Object item)
			throws TransactionException {
		// TODO Auto-generated method stub
		
	}

	public Object getObject(CacheTransactionManager manager, Cache cache,
			String key, boolean forUpdate, long time)
			throws TransactionException {
		// TODO Auto-generated method stub
		return null;
	}

	public void put(CacheTransactionManager manager, Cache cache, String key,
			long maxAliveTime, InputStream inputData)
			throws TransactionException {
		// TODO Auto-generated method stub
		
	}

	public InputStream get(CacheTransactionManager manager, Cache cache,
			String key, boolean forUpdate, long time)
			throws TransactionException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean remove(CacheTransactionManager manager, Cache cache,
			String key) throws TransactionException {
		// TODO Auto-generated method stub
		return false;
	}
	
}
