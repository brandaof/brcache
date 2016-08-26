package org.brandao.brcache;

import java.util.concurrent.atomic.AtomicLong;

public class PutStressThreadHelper extends Thread{
	
	private Cache cache;
	
	private AtomicLong key;
	
	private String value;
	
	private Throwable error;
	
	public PutStressThreadHelper(AtomicLong key, String value, Cache cache){
		this.key   = key;
		this.value = value;
		this.cache = cache;
	}
	
	public void run(){
		while(this.key.get() < 1000 && this.error == null){
			try{
		        long k = this.key.getAndIncrement();
		        String key = String.valueOf(k);
		        String value = key + this.value;
		        cache.put(key, value, 0);
			}
			catch(Throwable e){
				this.error = e;
			}
		}
	}
	
	public boolean isOnline(){
		return this.key.get() < 1000 && this.error == null;
	}
	
}
