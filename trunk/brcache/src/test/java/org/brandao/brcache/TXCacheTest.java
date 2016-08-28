package org.brandao.brcache;

import org.brandao.brcache.TXCacheHelper.ConcurrentTask;
import org.brandao.brcache.tx.CacheTransaction;

import junit.framework.TestCase;

public class TXCacheTest extends TestCase{

	private static final String KEY    = "teste";

	private static final String VALUE  = "value";

	private static final String VALUE2 = "val";
	
	/* replace */
	
	public void testReplace() throws StorageException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0, 0));
	}

	public void testReplaceSuccess() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testReplaceExact() throws StorageException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
	}

	public void testReplaceExactSuccess() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* putIfAbsent */
	
	public void testputIfAbsent() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentExistValue() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* put */
	
	public void testPut() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* get */
	
	public void testGet() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testGetOverride() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		cache.put(KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* remove */
	
	public void testRemoveExact() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY, VALUE));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertFalse(cache.remove(KEY, VALUE2));
		TestCase.assertTrue(cache.remove(KEY, VALUE));
		TestCase.assertNull(cache.get(KEY));
	}

	public void testRemove() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		
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
		TXCache cache = new Cache().getTXCache();
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0, 0));
		tx.commit();
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0, 0));
	}

	public void testExplicitTransactionReplaceSuccess() throws Throwable{
		TXCache cache = new Cache().getTXCache();
		CacheTransaction tx = cache.beginTransaction();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testExplicitTransactionReplaceExact() throws Throwable{
		TXCache cache = new Cache().getTXCache();
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		tx.commit();
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
	}

	public void testExplicitTransactionReplaceExactSuccess() throws Throwable{
		TXCache cache = new Cache().getTXCache();
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
		TXCache cache = new Cache().getTXCache();
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testExplicitTransactionPutIfAbsentExistValue() throws Throwable{
		TXCache cache = new Cache().getTXCache();
		CacheTransaction tx = cache.beginTransaction();
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* put */
	
	public void testExplicitTransactionPut() throws Throwable{
		TXCache cache = new Cache().getTXCache();
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* get */
	
	public void testExplicitTransactionGet() throws Throwable{
		TXCache cache = new Cache().getTXCache();
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testExplicitTransactionGetOverride() throws Throwable{
		TXCache cache = new Cache().getTXCache();
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
		TXCache cache = new Cache().getTXCache();
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
		TXCache cache = new Cache().getTXCache();
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
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
				cache.put(key, value, 0, 0);
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
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
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

	public void testConcurrentTransactionReplaceExact() throws Throwable{
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
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
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
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
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		cache.get(KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testConcurrentTransactionPutIfAbsentExistValue() throws Throwable{
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* put */
	
	public void testConcurrentTransactionPut() throws Throwable{
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* get */
	
	public void testConcurrentTransactionGet() throws Throwable{
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testConcurrentTransactionGetOverride() throws Throwable{
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		cache.put(KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* remove */
	
	public void testConcurrentTransactionRemoveExact() throws Throwable{
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
				cache.put(key, value2, 0, 0);
			}
			
		};
		
		CacheTransaction tx = cache.beginTransaction();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY, VALUE));
		cache.put(KEY, VALUE, 0, 0);

		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertFalse(cache.remove(KEY, VALUE2));
		TestCase.assertTrue(cache.remove(KEY, VALUE));
		TestCase.assertNull(cache.get(KEY));
		tx.commit();
		
		Thread.sleep(1000);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testConcurrentTransactionRemove() throws Throwable{
		TXCache cache = new Cache().getTXCache();

		ConcurrentTask task = new ConcurrentTask(cache, KEY, VALUE, VALUE2){

			@Override
			protected void execute(TXCache cache, String value, String key,
					String value2) throws Throwable {
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
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}
	
}
