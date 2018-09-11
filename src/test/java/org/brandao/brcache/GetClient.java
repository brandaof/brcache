package org.brandao.brcache;

import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class GetClient implements Runnable{
	
	private static final Random random = new Random();
	
	private Cache cache;
	
	private AtomicInteger keyCount;
	
	private byte[] buffer;
	
	private Throwable error;
	
	private int maxOperations;
	
	private CountDownLatch countDown;
	
	public GetClient(Cache cache, 
			AtomicInteger keyCount, int maxOperations, CountDownLatch countDown){
		this.keyCount      = keyCount;
		this.maxOperations = maxOperations;
		this.buffer        = new byte[1024];
		this.cache         = cache;
	}
	
	@SuppressWarnings("unused")
	public void run(){
		for(int i=0;i<maxOperations;i++){
			try{
				long key       = random.nextInt(keyCount.get());
				String strKey  = Long.toString(key, Character.MAX_RADIX);
				InputStream in = cache.getStream(strKey);
				if(in != null){
					int l;
					try{
						while((l = in.read(buffer, 0, buffer.length)) != -1);
					}
					finally{
						in.close();
					}
				}
			}
			catch(Throwable e){
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
