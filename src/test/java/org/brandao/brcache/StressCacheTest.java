package org.brandao.brcache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

/**
 *
 * @author Brandao
 */
public class StressCacheTest extends TestCase{
    
	private Cache cache;
	
	public void setUp(){
		BRCacheConfig config = new TestBRCacheConfig();
		this.cache = new ConcurrentCache(config);
	}
	
	public void tearDown(){
		this.cache.destroy();
		this.cache = null;
		System.gc();
	}
	
	public void testPut() throws InterruptedException{
		int totalClients  = 1000;
		int maxOperations = 1000;
		CountDownLatch countDown = new CountDownLatch(totalClients);
		AtomicInteger keyCount   = new AtomicInteger();
		Thread[] clients         = new Thread[totalClients];
		byte[] value             = new byte[1024];
		
		for(int i=0;i<totalClients;i++){
			clients[i] = new Thread(new PutClient(cache, keyCount, maxOperations, countDown, value));
		}
		
		long time = System.nanoTime();
		for(Thread c: clients){
			c.start();
		}
		countDown.await();
		time = System.nanoTime() - time;
		
		double op     = totalClients*maxOperations;
		double timeOp = time / op;
		double opsSec = 1000000000 / timeOp;
		
		System.out.println("operations: " + op + ", time: " + time + " nano, ops: " + op + ", ops/Sec: " + + opsSec );
		
		Thread.sleep(6000);
	}

}
