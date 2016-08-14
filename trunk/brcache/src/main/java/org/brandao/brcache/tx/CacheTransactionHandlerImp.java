package org.brandao.brcache.tx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.brandao.brcache.Cache;
import org.brandao.database.EntityFile;
import org.brandao.database.transaction.TransactionInfo;

public class CacheTransactionHandlerImp 
	implements CacheTransactionHandler{

	private static final String TRANSACTION_NAME = "{{name}}.tx";
	
	private TransactionInfo transactionInfo;
	
	private boolean commitInProgress;
	
	private boolean started;

	private boolean rolledBack;
	
	private boolean commited;
	
	private Cache cache;
	
	private CacheTransactionManager transactionManager;
	
	private File file;

	private String transactionName;
	
	private CacheTransactionHandlerImp(UUID id, 
			CacheTransactionManager transactionManager, Cache cache){
		this.commitInProgress   = false;
		this.started            = false;
		this.commited           = false;
		this.rolledBack         = false;
		this.transactionInfo    = new TransactionInfo(id);
		this.cache				= cache;
		this.transactionManager	= transactionManager;
		this.transactionName    = id.toString();
		this.file               = new File(
				transactionManager.getTransactionPath(), 
				TRANSACTION_NAME.replace("{{name}}", this.transactionName));
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
		this.rollback(this.transactionInfo);
	}
	
	private void rollback(TransactionInfo transactionInfo) throws TransactionException {

		if(this.rolledBack)
			throw new TransactionException("transaction has been rolled back");

		if(this.commited)
			throw new TransactionException("transaction has been commited");
		
		if(!started)
			throw new TransactionException("transaction not started");
			
		try{
			if(this.file.exists()){
				TransactionInfo localTransactionInfo = 
						this.readPersistedTransaction(this.file);
				this.rollback(localTransactionInfo);
				this.clearTransaction(localTransactionInfo);
			}
			else{
				if(this.commitInProgress){
					this.transactionInfo.rollback(this.cache);
					this.clearTransaction(this.transactionInfo);
				}
				else{
					this.clearTransaction(this.transactionInfo);
					return;
				}
			}
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		
	}
	
	private TransactionInfo readPersistedTransaction(File file) throws IOException{
		FileInputStream stream = null;
		ObjectInputStream objStream = null;
		try{
			stream = new FileInputStream(file);
			objStream = new ObjectInputStream(stream);
			return (TransactionInfo) objStream.readObject();
		}
		catch(ClassNotFoundException e){
			throw new IOException(e);
		}
		finally{
			if(stream != null)
				stream.close();
		}
	}
	
	private void persistTransaction(File file, TransactionInfo transactionInfo) throws IOException{
		FileOutputStream stream = null;
		ObjectOutputStream objStream = null;
		try{
			stream = new FileOutputStream(file);
			objStream = new ObjectOutputStream(stream);
			objStream.writeObject(transactionInfo);
			objStream.flush();
		}
		finally{
			if(stream != null){
				stream.flush();
				stream.close();
			}
		}
		
		this.commitInProgress = true;
	}
	
	protected void clearTransaction(TransactionInfo transactionInfo) throws TransactionException{
		file.delete();
		transactionInfo.clear();
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
			
			this.persistTransaction(file, this.transactionInfo);
			try{
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
