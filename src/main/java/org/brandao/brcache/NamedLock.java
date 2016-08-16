package org.brandao.brcache;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NamedLock {

	private Map<String,Set<UUID>> origins;

	private Map<String,Lock> locks;
	
	public NamedLock(){
		this.origins = new HashMap<String, Set<UUID>>();
		this.locks  = new HashMap<String, Lock>();
	}
	
	public Serializable lock(String lockName){
		
		Lock lock = null;
		UUID ref  = UUID.randomUUID();
		synchronized(this){
			Set<UUID> originSet = this.origins.get(lockName);
			lock = this.locks.get(lockName);
			
			if(originSet == null){
				originSet = new HashSet<UUID>();
				this.origins.put(lockName, originSet);
				
				if(lock != null){
					throw new IllegalStateException("bug!");
				}
				
				lock = new ReentrantLock();
				this.locks.put(lockName, lock);
			}
			
		}
		
		lock.lock();
		return ref;
	}
	
	public void unlock(Serializable ref, String lockName){
		Lock lock = null;
		synchronized(this){
			Set<UUID> originSet = this.origins.get(lockName);
			
			if(originSet == null){
				throw new IllegalStateException("invalid lock reference: " + lockName + ": " + ref);
			}
			
			if(!originSet.remove(ref)){
				throw new IllegalStateException("lock reference not found: " + lockName + ": " + ref);
			}
			
			lock = this.locks.get(lockName);
			
			if(originSet.isEmpty() && this.locks.remove(lockName) == null){
				throw new IllegalStateException("lock not found: " + lockName + ": " + ref);
			}
		}
		
		lock.unlock();
		
	}	
}
