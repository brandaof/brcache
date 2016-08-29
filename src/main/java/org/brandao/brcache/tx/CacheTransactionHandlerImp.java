package org.brandao.brcache.tx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.StreamCache;

class CacheTransactionHandlerImp
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
	
	private BRCacheTransactionConfig cacheTransactionConfig;
	
	public CacheTransactionHandlerImp(
			BRCacheTransactionConfig cacheTransactionConfig,
			UUID id, 
			CacheTransactionManager transactionManager, 
			StreamCache cache){
		
		this.commitInProgress   = false;
		this.started            = false;
		this.commited           = false;
		this.rolledBack         = false;
		this.cache				= cache;
		this.transactionManager	= transactionManager;
		this.transactionName    = id.toString();
		this.id					= id;
		this.cacheTransactionConfig = cacheTransactionConfig;
	}
	
	public Serializable getId() {
		return this.id;
	}
	
	public synchronized void begin() {
		
		//this.transactionInfo = new TransactionInfo(id, transactionManager.getTransactionPath());
		this.transactionInfo = new TransactionInfo(
				id, this.cacheTransactionConfig);
		
		this.file            = new File(
				transactionManager.getTransactionPath(), 
				TRANSACTION_NAME.replace("{{name}}", this.transactionName));
		
		if(this.started){
			throw new TransactionException(CacheErrors.ERROR_1016);
		}

		if(this.commitInProgress){
			throw new TransactionException(CacheErrors.ERROR_1010);
		}
		
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
			if(this.file.exists()){
				TransactionInfo localTransactionInfo = 
						this.readPersistedTransaction(this.file);
				this.rollback(localTransactionInfo);
				this.closeTransaction(localTransactionInfo);
			}
			else{
				if(this.commitInProgress){
					this.transactionInfo.rollback(this.cache);
					this.closeTransaction(this.transactionInfo);
				}
				else{
					this.closeTransaction(this.transactionInfo);
					return;
				}
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
	
	protected void closeTransaction(TransactionInfo transactionInfo) throws TransactionException{
		this.transactionManager.close(this);
	}
	
	public void close(){
		try{
			transactionInfo.close();
			file.delete();
			this.cache 				= null;
			this.file 				= null;
			this.transactionInfo 	= null;
			this.transactionManager = null;
			this.started 			= false;
			this.commitInProgress 	= false;
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
			this.transactionInfo.savePoint(this.cache);
			this.persistTransaction(file, this.transactionInfo);
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

	public Object replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long timeToLive, long timeToIdle, long time)
			throws StorageException {
		return this.transactionInfo.replace(manager, cache, key, value, timeToLive, timeToIdle, time);
	}

	public boolean replace(CacheTransactionManager manager, StreamCache cache,
			String key, Object oldValue, Object newValue, long timeToLive, long timeToIdle,
			long time) throws StorageException {
		return this.transactionInfo.replace(manager, cache, key, oldValue, newValue, timeToLive, timeToIdle, time);
	}

	public Object putIfAbsent(CacheTransactionManager manager,
			StreamCache cache, String key, Object value, long timeToLive, long timeToIdle,
			long time) throws StorageException {
		return this.transactionInfo.putIfAbsent(manager, cache, key, value, timeToLive, timeToIdle, time);
	}

	public boolean put(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long timeToLive, long timeToIdle, long time)
			throws StorageException {
		return this.transactionInfo.put(manager, cache, key, value, timeToLive, timeToIdle, time);
	}

	public boolean putStream(CacheTransactionManager manager, StreamCache cache,
			String key, long timeToLive, long timeToIdle, InputStream inputData, long time)
			throws StorageException {
		return this.transactionInfo.putStream(manager, cache, key, timeToLive, timeToIdle, inputData, time);
	}

	public Object get(CacheTransactionManager manager, StreamCache cache,
			String key, boolean forUpdate, long time) throws RecoverException {
		return this.transactionInfo.get(manager, cache, key, forUpdate, time);
	}

	public InputStream getStream(CacheTransactionManager manager,
			StreamCache cache, String key, boolean forUpdate, long time)
			throws RecoverException {
		return this.transactionInfo.getStream(manager, cache, key, forUpdate, time);
	}

	public boolean remove(CacheTransactionManager manager, StreamCache cache,
			String key, Object value, long time) throws StorageException {
		return this.transactionInfo.remove(manager, cache, key, value, time);
	}

	public boolean remove(CacheTransactionManager manager, StreamCache cache,
			String key, long time) throws StorageException {
		return this.transactionInfo.remove(manager, cache, key, time);
	}

}
