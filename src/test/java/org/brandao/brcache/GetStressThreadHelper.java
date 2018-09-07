package org.brandao.brcache;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;
import junit.framework.TestCase;

public class GetStressThreadHelper extends Thread{
	
	private ConcurrentCache cache;
	
	private AtomicLong key;
	
	private String value;
	
	private Throwable error;
	
	private long count;
	
	public GetStressThreadHelper(AtomicLong key, String value, ConcurrentCache cache){
		this.key   = key;
		this.value = value;
		this.cache = cache;
		this.count = 0;
	}
	
	public void run(){
		Random random = new Random();
		while(this.count++ < 1000 && this.error == null){
			try{
		        long k = random.nextInt(this.key.intValue());
		        String key = String.valueOf(k);
		        String value = key + this.value;
		        String cachedValue = (String) cache.get(key);
		        
                if(cachedValue != null){
                    TestCase.assertEquals(value, cachedValue);
                }

                Thread.sleep(200);
			}
			catch(Throwable e){
				this.error = e;
			}
		}
	}
	
	public boolean isOnline(){
		return this.key.get() < 1000000L && this.error == null;
	}
	
}
