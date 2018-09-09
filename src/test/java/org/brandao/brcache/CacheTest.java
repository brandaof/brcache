package org.brandao.brcache;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.TestCase;

public class CacheTest extends TestCase{

	private static final String KEY    = "teste";

	private static final String VALUE  = "value";

	private static final String VALUE2 = "val";
	
	private static final BRCacheConfig config = new TestBRCacheConfig();
	
	/* replace */
	
	public void testReplace() throws StorageException{
		ConcurrentCache cache = new ConcurrentCache(config);
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0, 0));
	}

	public void testReplaceSuccess() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testReplaceStream() throws StorageException, IOException{
		ConcurrentCache cache = new ConcurrentCache(config);
		TestCase.assertFalse(cache.replaceStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0));
	}

	public void testReplaceStreamSuccess() throws StorageException, RecoverException, IOException, ClassNotFoundException{
		ConcurrentCache cache = new ConcurrentCache(config);
		cache.putStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		TestCase.assertEquals(VALUE, (String)CacheTestHelper.toObject(cache.getStream(KEY)));
		TestCase.assertTrue(cache.replaceStream(KEY, CacheTestHelper.toStream(VALUE2), 0, 0));
		TestCase.assertEquals(VALUE2, (String)CacheTestHelper.toObject(cache.getStream(KEY)));
	}
	
	public void testReplaceExact() throws StorageException{
		ConcurrentCache cache = new ConcurrentCache(config);
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0, 0));
	}

	public void testReplaceExactSuccess() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* putIfAbsent */
	
	public void testputIfAbsent() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentExistValue() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentStream() throws StorageException, RecoverException, IOException{
		ConcurrentCache cache = new ConcurrentCache(config);
		TestCase.assertNull(cache.putIfAbsentStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentStreamExistValue() throws StorageException, RecoverException, IOException, ClassNotFoundException{
		ConcurrentCache cache = new ConcurrentCache(config);
		cache.putStream(KEY, CacheTestHelper.toStream(VALUE), 0, 0);
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.putIfAbsentStream(KEY, CacheTestHelper.toStream(VALUE2), 0, 0)));
		TestCase.assertEquals(VALUE, CacheTestHelper.toObject(cache.getStream(KEY)));
	}
	
	/* put */
	
	public void testPut() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* get */
	
	public void testGet() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testGetOverride() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		cache.put(KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* remove */
	
	public void testRemoveExact() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY, VALUE));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertFalse(cache.remove(KEY, VALUE2));
		TestCase.assertTrue(cache.remove(KEY, VALUE));
	}

	public void testRemove() throws StorageException, RecoverException{
		ConcurrentCache cache = new ConcurrentCache(config);
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY));
		
		cache.put(KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertTrue(cache.remove(KEY));
	}

	/* timeToLive */
	
	public void testTimeToLive() throws InterruptedException{
		ConcurrentCache cache = new ConcurrentCache(config);
		cache.put(KEY, VALUE, 1000, 0);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(800);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(400);
		assertNull(cache.get(KEY));
	}

	public void testTimeToLiveLessThanTimeToIdle() throws InterruptedException{
		ConcurrentCache cache = new ConcurrentCache(config);
		cache.put(KEY, VALUE, 1000, 5000);
		assertEquals(cache.get(KEY), VALUE);
		Thread.sleep(1200);
		assertNull(cache.get(KEY));
	}

	public void testNegativeTimeToLive() throws InterruptedException{
		try{
			ConcurrentCache cache = new ConcurrentCache(config);
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
		ConcurrentCache cache = new ConcurrentCache(config);
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
		ConcurrentCache cache = new ConcurrentCache(config);
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
			ConcurrentCache cache = new ConcurrentCache(config);
			cache.put(KEY, VALUE, 0, -1);
			fail();
		}
		catch(StorageException e){
			if(!e.getError().equals(CacheErrors.ERROR_1028)){
				fail();
			}
				
		}
	}
	
    public void testswap() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
    	
        final int itens   = 1000;
        final Cache cache = new ConcurrentCache(config);
        
        for(int i=0;i<itens;i++){
            try{
                String key = String.valueOf(i) + "-INDEX-AJBK-";
                String value = key + "-INDEX-AJBK-";
                cache.put(key, value, 0, 0);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }

        for(int i=0;i<itens;i++){
            try{
                String key = String.valueOf(i) + "-INDEX-AJBK-";
                String value = key + "-INDEX-AJBK-";
                String cachedValue = (String) cache.get(key);
                assertEquals(value, cachedValue);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        
    }    
    
}
