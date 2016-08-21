package org.brandao.brcache;

public class TXCacheHelper {

	public static abstract class ConcurrentTask extends Thread{
		
		private Throwable error;
		
		private TXCache cache;
		
		private String value;
		
		private String key;
		
		private String value2;
		
		public ConcurrentTask(TXCache cache, String value, String key,
				String value2) {
			this.cache = cache;
			this.value = value;
			this.key = key;
			this.value2 = value2;
		}

		public void run(){
			try{
				this.execute(cache, value, key, value2);
			}
			catch(Throwable e){
				this.error = e; 
			}
		}

		protected abstract void execute(TXCache cache, String value, String key,
				String value2) throws Throwable;
		
		public Throwable getError() {
			return error;
		}
		
	}
}
