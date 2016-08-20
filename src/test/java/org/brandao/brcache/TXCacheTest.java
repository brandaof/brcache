package org.brandao.brcache;

import junit.framework.TestCase;

public class TXCacheTest extends TestCase{

	private static final String KEY    = "teste";

	private static final String VALUE  = "value";

	private static final String VALUE2 = "val";
	
	/* replace */
	
	public void testReplace() throws StorageException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertFalse(cache.replace(KEY, VALUE, 0));
	}

	public void testReplaceSuccess() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		cache.put(KEY, VALUE, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE2, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	public void testReplaceExact() throws StorageException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertFalse(cache.replace(KEY, VALUE, VALUE2, 0));
	}

	public void testReplaceExactSuccess() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		cache.put(KEY, VALUE, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		TestCase.assertTrue(cache.replace(KEY, VALUE, VALUE2, 0));
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* putIfAbsent */
	
	public void testputIfAbsent() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertNull(cache.putIfAbsent(KEY, VALUE, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testputIfAbsentExistValue() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		cache.put(KEY, VALUE, 0);
		TestCase.assertEquals(VALUE, cache.putIfAbsent(KEY, VALUE2, 0));
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* put */
	
	public void testPut() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	/* get */
	
	public void testGet() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
	}

	public void testGetOverride() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		TestCase.assertNull((String)cache.get(KEY));
		cache.put(KEY, VALUE, 0);
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		cache.put(KEY, VALUE2, 0);
		TestCase.assertEquals(VALUE2, (String)cache.get(KEY));
	}

	/* remove */
	
	public void testRemoveExact() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY, VALUE));
		
		cache.put(KEY, VALUE, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertFalse(cache.remove(KEY, VALUE2));
		TestCase.assertTrue(cache.remove(KEY, VALUE));
	}

	public void testRemove() throws StorageException, RecoverException{
		TXCache cache = new Cache().getTXCache();
		
		TestCase.assertNull((String)cache.get(KEY));
		TestCase.assertFalse(cache.remove(KEY));
		
		cache.put(KEY, VALUE, 0);
		
		TestCase.assertEquals(VALUE, (String)cache.get(KEY));
		
		TestCase.assertTrue(cache.remove(KEY));
	}
	
	/* with Transaction */
	
}
