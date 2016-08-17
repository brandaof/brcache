package org.brandao.brcache.tx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

import org.brandao.brcache.StreamCache;

public class CacheTransactionHandlerImp
	extends TransactionInfo 
	implements CacheTransactionHandler{

	private static final String TRANSACTION_NAME = "{{name}}.tx";
	
	private TransactionInfo transactionInfo;
	
	private boolean commitInProgress;
	
	private boolean started;

	private boolean rolledBack;
	
	private boolean commited;
	
	private StreamCache cache;
	
	private CacheTransactionManager transactionManager;
	
	private File file;

	private String transactionName;
	
	private UUID id;
	
	public CacheTransactionHandlerImp(UUID id, 
			CacheTransactionManager transactionManager, StreamCache cache){
		super(id);
		this.commitInProgress   = false;
		this.started            = false;
		this.commited           = false;
		this.rolledBack         = false;
		this.transactionInfo    = new TransactionInfo(id);
		this.cache				= cache;
		this.transactionManager	= transactionManager;
		this.transactionName    = id.toString();
		this.id					= id;
		this.file               = new File(
				transactionManager.getTransactionPath(), 
				TRANSACTION_NAME.replace("{{name}}", this.transactionName));
	}
	
	public Serializable getId() {
		return this.id;
	}
	
	public synchronized void begin() throws TransactionException {
		
		if(this.started)
			throw new TransactionException("transaction has been started");

		if(this.commitInProgress)
			throw new TransactionException("commit in progress");
		
		this.started = true;
	}
	
	public boolean isRolledBack() {
		return this.rolledBack;
	}

	public boolean isCommited() {
		return this.commited;
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
			this.rolledBack = true;
			this.commited   = false;
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
			this.commitInProgress = true;
			this.transactionInfo.savePoint(this.cache);
			this.persistTransaction(file, this.transactionInfo);
			this.transactionInfo.commit(cache);
			this.clearTransaction(this.transactionInfo);
			this.commitInProgress = true;
			this.commited         = true;
			this.rolledBack       = false;
		}
		catch (Throwable e) {
			throw new TransactionException(e);
		}
	}

}