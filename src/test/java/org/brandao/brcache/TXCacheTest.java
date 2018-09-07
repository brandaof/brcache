package org.brandao.brcache;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.brandao.brcache.TXCacheHelper.ConcurrentTask;
import org.brandao.brcache.tx.CacheTransaction;
import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.CacheTransactionManagerImp;
import org.brandao.brcache.tx.TXCache;

import junit.framework.TestCase;

public class TXCacheTest extends TestCase{

	private static final String KEY    = "teste";

	private static final String VALUE  = "value";

	private static final String VALUE2 = "val";
	
	private static final BRCacheConfig config = new TestBRCacheConfig();
	
	private static final CacheTransactionManager txm = new CacheTransactionManagerImp("./tx", TimeUnit.SECONDS.toMillis(30));
	/* replace */
	
	public void testReplace() throws StorageException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0, 0));
	}

	public void testReplaceSuccess() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testReplaceStream() throws StorageException, IOException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		TestCase.assertFalse(cache.replaceStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0));
	}

	public void testReplaceStreamSuccess() throws StorageException, RecoverException, IOException, ClassNotFoundException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.putStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
		TestCase.assertTrue(cache.replaceStream(KEY, CacheTestHelper.toStream(VALUE2), 0, 0));
		TestCase.assertEquals(VALUE2, CacheTestHelper.toObject(cache.getStream(KEY)));
	}
	
	public void testReplaceExact() throws StorageException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
	}

	public void testReplaceExactSuccess() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* putIfAbsent */
	
	public void testputIfAbsent() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentExistValue() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentStream() throws StorageException, RecoverException, IOException, ClassNotFoundException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		TestCase.assertNull(cache.putIfAbsentStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0));
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
	}

	public void testputIfAbsentStreamExistValue() throws StorageException, RecoverException, IOException, ClassNotFoundException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.putStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.putIfAbsentStream(KEY, CacheTestHelper.toStream(VALUE2), 0, 0)));
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
	}
	
	/* put */
	
	public void testPut() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* get */
	
	public void testGet() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testGetOverride() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		cache.put(KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* remove */
	
	public void testRemoveExact() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY, VALUE));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertFalse(cache.remove(KEY, VALUE2));
		TestCase.assertTrue(cache.remove(KEY, VALUE));
		TestCase.assertNull(cache.get(KEY));
	}

	public void testRemove() throws StorageException, RecoverException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertTrue(cache.remove(KEY));
		TestCase.assertNull(cache.get(KEY));
	}
	
	/* with explicit transaction */

	/* replace */
	
	public void testExplicitTransactionReplace() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0, 0));
		tx.commit();
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0, 0));
	}

	public void testExplicitTransactionReplaceSuccess() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testExplicitTransactionReplaceExact() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		tx.commit();
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
	}

	public void testExplicitTransactionReplaceExactSuccess() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* putIfAbsent */
	
	public void testExplicitTransactionPutIfAbsent() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testExplicitTransactionPutIfAbsentExistValue() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* put */
	
	public void testExplicitTransactionPut() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* get */
	
	public void testExplicitTransactionGet() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testExplicitTransactionGetOverride() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		cache.put(KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* remove */
	
	public void testExplicitTransactionRemoveExact() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY, VALUE));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertFalse(cache.remove(KEY, VALUE2));
		TestCase.assertTrue(cache.remove(KEY, VALUE));
		tx.commit();
		
		TestCase.assertFalse(cache.remove(KEY, VALUE));
	}

	public void testExplicitTransactionRemove() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		CacheTransaction tx = cache.beginTransaction();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertTrue(cache.remove(KEY));
		TestCase.assertNull(cache.get(KEY));
		tx.commit();
		
		TestCase.assertNull(cache.get(KEY));
	}	
	
	/* concurrent transaction*/
	
	/* replace */
	
	public void testConcurrentTransactionReplace() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, CacheTestHelper.toStream(VALUE), CacheTestHelper.toStream(VALUE2)){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.putStream(key, (InputStream)value, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull(cache.get(KEY, true));
		task.start();
		Thread.sleep(2000);
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0, 0));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, cache.get(KEY));
	}

	public void testConcurrentTransactionReplaceSuccess() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
		
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testConcurrentTransactionReplaceStream() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, CacheTestHelper.toStream(VALUE), CacheTestHelper.toStream(VALUE2)){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.putStream(key, (InputStream)value, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull(cache.getStream(KEY, true));
		task.start();
		Thread.sleep(2000);
		TestCase.assertFalse(cache.replaceStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
	}

	public void testConcurrentTransactionReplaceStreamSuccess() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, CacheTestHelper.toStream(VALUE), CacheTestHelper.toStream(VALUE2)){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.putStream(key, (InputStream)value, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		
		cache.putStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
		TestCase.assertTrue(cache.replaceStream(KEY, CacheTestHelper.toStream(VALUE2), 0, 0));
		TestCase.assertEquals(VALUE2, CacheTestHelper.toObject(cache.getStream(KEY)));
		
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
	}
	
	public void testConcurrentTransactionReplaceExact() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		cache.get(KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testConcurrentTransactionReplaceExactSuccess() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* putIfAbsent */
	
	public void testConcurrentTransactionPutIfAbsent() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		cache.get(KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, cache.get(KEY));
	}

	public void testConcurrentTransactionPutIfAbsentExistValue() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, cache.get(KEY));
	}

	public void testConcurrentTransactionPutIfAbsentStream() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, CacheTestHelper.toStream(VALUE), CacheTestHelper.toStream(VALUE2)){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.putStream(key, (InputStream)value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		cache.getStream(KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertNull(cache.putIfAbsentStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0));
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, CacheTestHelper.toObject(cache.getStream(KEY)));
	}

	public void testConcurrentTransactionPutIfAbsentStreamExistValue() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		cache.putStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.putIfAbsentStream(KEY, CacheTestHelper.toStream(VALUE2), 0, 0)));
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, CacheTestHelper.toObject(cache.getStream(KEY)));
	}	
	/* put */
	
	public void testConcurrentTransactionPut() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, cache.get(KEY));
	}

	/* get */
	
	public void testConcurrentTransactionGet() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, cache.get(KEY));
	}

	public void testConcurrentTransactionGetOverride() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull(cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, cache.get(KEY));
		cache.put(KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, cache.get(KEY));
	}

	/* remove */
	
	public void testConcurrentTransactionRemoveExact() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY, VALUE));
		cache.put(KEY, VALUE, 0, 0);

		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, cache.get(KEY));
		
		TestCase.assertFalse(cache.remove(KEY, VALUE2));
		TestCase.assertTrue(cache.remove(KEY, VALUE));
		TestCase.assertNull(cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, cache.get(KEY));
	}

	public void testConcurrentTransactionRemove() throws Throwable{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String key, Object value,
					Object value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY));
		cache.put(KEY, VALUE, 0, 0);

		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.remove(KEY));
		TestCase.assertNull(cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, cache.get(KEY));
	}

	/* timeToLive */
	
	public void testTimeToLive() throws InterruptedException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.put(KEY, VALUE, 1000, 0);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(400);
		assertNull(cache.get(KEY));
	}

	public void testTimeToLiveLessThanTimeToIdle() throws InterruptedException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.put(KEY, VALUE, 1000, 5000);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(1200);
		assertNull(cache.get(KEY));
	}

	public void testNegativeTimeToLive() throws InterruptedException{
		try{
			TXCache cache = new ConcurrentCache(config).getTXCache(txm);
			cache.put(KEY, VALUE, -1, 5000);
			fail();
		}
		catch(StorageException e){
			if(!e.getError().equals(CacheErrors.ERROR_1029)){
				fail();
			}
				
		}
	}

	/* TimeToIdle */
	
	public void testTimeToIdle() throws InterruptedException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.put(KEY, VALUE, 0, 1000);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(1200);
		assertNull(cache.get(KEY));
		
	}

	public void testTimeToIdleLessThanTimeToLive() throws InterruptedException{
		TXCache cache = new ConcurrentCache(config).getTXCache(txm);
		cache.put(KEY, VALUE, 20000, 1000);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(1200);
		assertNull(cache.get(KEY));
	}

	public void testNegativeTimeToIdle() throws InterruptedException{
		try{
			TXCache cache = new ConcurrentCache(config).getTXCache(txm);
			cache.put(KEY, VALUE, 0, -1);
			fail();
		}
		catch(StorageException e){
			if(!e.getError().equals(CacheErrors.ERROR_1028)){
				fail();
			}
				
		}
	}
	
}
