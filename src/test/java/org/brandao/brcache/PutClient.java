package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class PutClient implements Runnable{
	
	private Cache cache;
	
	private AtomicInteger keyCount;
	
	private Throwable error;
	
	private int maxOperations;
	
	private CountDownLatch countDown;
	
	private byte[] value;
	
	public PutClient(Cache cache, 
			AtomicInteger keyCount, int maxOperations, CountDownLatch countDown, byte[] value){
		this.keyCount      = keyCount;
		this.maxOperations = maxOperations;
		this.cache         = cache;
		this.value         = value;
		this.countDown     = countDown;
	}
	
	public void run(){
		for(int i=0;i<maxOperations;i++){
			try{
				long key      = keyCount.getAndIncrement();
				String strKey = Long.toString(key, Character.MAX_RADIX);
				cache.putStream(strKey, new ByteArrayInputStream(value), 0, 0);
			}
			catch(Throwable e){
				e.printStackTrace();
				error = e;
				break;
			}
		}
		
		countDown.countDown();
		
	}

	public Throwable getError() {
		return error;
	}
	
}
