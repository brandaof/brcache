package org.brandao.brcache;

import org.brandao.brcache.tx.TXCache;

public class TXCacheHelper {

	public static abstract class ConcurrentTask extends Thread{
		
		private Throwable error;
		
		private TXCache cache;
		
		private Object value;
		
		private String key;
		
		private Object value2;
		
		public ConcurrentTask(TXCache cache, String key,
				Object value, Object value2) {
			this.cache = cache;
			this.value = value;
			this.key = key;
			this.value2 = value2;
		}

		public void run(){
			try{
				this.execute(cache, key, value, value2);
			}
			catch(Throwable e){
				this.error = e; 
			}
		}

		protected abstract void execute(TXCache cache, String key, Object value,
				Object value2) throws Throwable;
		
		public Throwable getError() {
			return error;
		}
		
	}
}
