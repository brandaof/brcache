package org.brandao.brcache;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NamedLock {

	private Map<String,Set<UUID>> origins;

	private Map<String,Lock> locks;

	private Lock _lock;
	
	public NamedLock(){
		this.origins = new HashMap<String, Set<UUID>>();
		this.locks  = new HashMap<String, Lock>();
		this._lock = new ReentrantLock();
	}
	
	public Serializable lock(String lockName){
		
		UUID ref  = UUID.randomUUID();
		Lock lock = this.getLock(ref, lockName);
		
		lock.lock();
		
		return ref;
	}

    public Serializable lockInterruptibly(String lockName) throws InterruptedException{
		UUID ref  = UUID.randomUUID();
		Lock lock = this.getLock(ref, lockName);
		
		lock.lockInterruptibly();
		
		return ref;
    }

	public Serializable tryLock(String lockName) throws InterruptedException{
		
		UUID ref  = UUID.randomUUID();
		Lock lock = this.getLock(ref, lockName);
		
		if(lock.tryLock()){
			return ref;
		}
		else{
			this.releaseLock(ref, lockName);
			return null;
		}
	}
    
	public Serializable tryLock(String lockName, long time, TimeUnit unit) throws InterruptedException{
		
		UUID ref  = UUID.randomUUID();
		Lock lock = this.getLock(ref, lockName);
		
		try{
			if(lock.tryLock(time, unit)){
				return ref;
			}
			else{
				this.releaseLock(ref, lockName);
				return null;
			}
		}
		catch(InterruptedException e){
			try{
				this.releaseLock(ref, lockName);
			}
			catch(Throwable x){
				throw new InterruptedException("bug: " + x.toString());
			}
			
			throw e;
		}
	}
	
	private Lock getLock(UUID ref, String lockName){
		_lock.lock();
		try{
			Set<UUID> originSet = this.origins.get(lockName);
			Lock lock = this.locks.get(lockName);
			
			if(originSet == null){
				originSet = new HashSet<UUID>();
				this.origins.put(lockName, originSet);
				
				if(lock != null){
					throw new IllegalStateException("bug!");
				}
				
				lock = new ReentrantLock();
				this.locks.put(lockName, lock);
			}
			return lock;
		}
		finally{
			_lock.unlock();
		}
	}
	
	public void unlock(Serializable ref, String lockName){
		Lock lock = this.locks.get(lockName);
		
		if(lock == null){
			throw new IllegalStateException("lock not found: " + lockName + ": " + ref);
		}
		
		lock.unlock();
		
		this.releaseLock(ref, lockName);
	}
	
	private void releaseLock(Serializable ref, String lockName){
		_lock.lock();
		try{
			Set<UUID> originSet = this.origins.get(lockName);
			
			if(originSet == null){
				throw new IllegalStateException("invalid lock reference: " + lockName + ": " + ref);
			}
			
			if(!originSet.remove(ref)){
				throw new IllegalStateException("lock reference not found: " + lockName + ": " + ref);
			}
			
			if(originSet.isEmpty() && this.locks.remove(lockName) == null){
				throw new IllegalStateException("lock not found: " + lockName + ": " + ref);
			}
			else{
				this.origins.remove(lockName);
			}
		}
		finally{
			_lock.unlock();
		}
	}
	
}
