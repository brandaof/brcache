package org.brandao.brcache;

class StatisticsThread implements Runnable{

    private long lastRead;
    
    private long lastWrite;
    
    private long read;
    
    private long write;
	
    private LockCache cache;
    
    private long currentReadings;
    
    private long currentWritten;
    
    private long currentTotalMemory;
    
    private long currentFreeMemory;
    
    private long currentMemory;
    
    public StatisticsThread(LockCache cache){
        this.lastRead  = 0;
        this.lastWrite = 0;
        this.read      = 0;
        this.write     = 0;
        this.cache     = cache;
    }
    
	public void run() {
        Runtime runtime = Runtime.getRuntime();
		while(true){
			try{
                this.currentWritten     = write-lastWrite;
                this.currentReadings    = (read-lastRead);
				this.currentTotalMemory = runtime.totalMemory();
				this.currentFreeMemory  = runtime.freeMemory();
				this.currentMemory      = runtime.totalMemory() - runtime.freeMemory();
                lastRead                = cache.getCountRead();
                lastWrite               = cache.getCountWrite();
                Thread.sleep(1000);
                read                    = cache.getCountRead();
                write                   = cache.getCountWrite();
				
			}
			catch(Throwable e){
				if(!(e instanceof InterruptedException)){
					e.printStackTrace();
				}
			}
		}
	}

	public long getCurrentReadings() {
		return currentReadings;
	}

	public long getCurrentWritten() {
		return currentWritten;
	}

	public long getCurrentTotalMemory() {
		return currentTotalMemory;
	}

	public long getCurrentFreeMemory() {
		return currentFreeMemory;
	}

	public long getCurrentMemory() {
		return currentMemory;
	}

}
