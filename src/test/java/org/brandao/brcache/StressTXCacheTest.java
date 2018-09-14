package org.brandao.brcache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.CacheTransactionManagerImp;

import junit.framework.TestCase;

/**
 *
 * @author Brandao
 */
public class StressTXCacheTest extends TestCase{
    
	private Cache cache;
	
	public void setUp(){
		BRCacheConfig config        = new TestBRCacheConfig();
		CacheTransactionManager txm = new CacheTransactionManagerImp("./tx", TimeUnit.SECONDS.toMillis(30));
		BasicCache ntxCache         = new ConcurrentCache(config);
		cache                       = ntxCache.getTXCache(txm);
	}
	
	public void tearDown(){
		this.cache.destroy();
		this.cache = null;
		System.gc();
	}
	
	public void testPut() throws InterruptedException{
		int totalClients  = 300;
		int maxOperations = 10;
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
	}

}
