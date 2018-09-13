package org.brandao.brcache.tx;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.CacheHandler;

public final class CacheHandlerTransactionFactory {

	public static TransactionCacheHandler createCacheHandler(
			CacheHandler cache, CacheTransactionManager transactionManager){
    	return (TransactionCacheHandler)Proxy.newProxyInstance(
    			cache.getClass().getClassLoader(), 
					new Class[]{TransactionCacheHandler.class}, 
					new CacheHandlerTransactionInvocationHandler(cache, transactionManager)
    			);
	}
	
	private static class CacheHandlerTransactionInvocationHandler implements InvocationHandler{

		private CacheHandler cache;
		
		private CacheTransactionManager transactionManager;
		
		public CacheHandlerTransactionInvocationHandler(
				CacheHandler cache, CacheTransactionManager transactionManager){
			this.cache = cache;
			this.transactionManager = transactionManager;
		}
		
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			CacheTransactionHandler currentTx = transactionManager.getCurrrent(false);
	    	CacheTransactionHandler tx        = 
	    			currentTx == null? 
	    					transactionManager.begin(cache) : 
	    					currentTx;
	    	
	    	try{
	    		Object r = method.invoke(tx, args);
				if(currentTx == null){
					tx.commit();
				}
				return r;
	    	}
	    	catch(InvocationTargetException e){
	    		
	    		Throwable ex = e.getTargetException();
	    		
	    		try{
	    			if(currentTx == null){
	    				tx.rollback();
	    			}
	    		}
				catch(Throwable x){
		    		throw new CacheException(
	    				new Exception(
							"bug: exception not recognized (rollback fail): " + x.toString(), ex), 
							CacheErrors.ERROR_1018);
					
				}
	    		
	    		if(ex instanceof CacheException){
	        		throw ex;
	    		}
	    		else{
	        		throw new CacheException(
	        				new Exception("bug: exception not recognized: ", e), 
	        				CacheErrors.ERROR_1023
    				);
	    		}
	    		
	    	}
    	}
		
	}
}
