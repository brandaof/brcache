package org.brandao.brcache;

public class StatisticsTask implements Runnable{

	private Cache cache;
	
	public StatisticsTask(Cache cache){
		this.cache = cache;
	}
	
    public void run(){
        long lastRead = 0;
        long lastWrite = 0;
        long read = 0;
        long write = 0;
        
        long lastDataRead = 0;
        long lastDataWrite = 0;
        long dataRead = 0;
        long dataWrite = 0;
        
        while(true){
            try{
                System.out.println("----------------------------------");
                System.out.println(
                    "write entry: " + (write-lastWrite) + "/sec " +
                    "read entry: " + (read-lastRead) + "/sec");

                System.out.println(
                        "write: " + (dataWrite-lastDataWrite)/1048576 + "m/sec " +
                        "read: " + (dataRead-lastDataRead)/1048576 + "m/sec");
                
                Runtime runtime = Runtime.getRuntime();
                runtime.gc();
                // Calculate the used memory
                long memory = runtime.totalMemory() - runtime.freeMemory();
                System.out.println("Used memory is bytes: " + memory);
                System.out.println("Used memory is megabytes: " + ((memory/1024L)/1024L));
                
                lastRead = cache.getCountRead();
                lastWrite = cache.getCountWrite();
                lastDataRead = cache.getCountReadData();
                lastDataWrite = cache.getCountWriteData();
                
                Thread.sleep(1000);
                read = cache.getCountRead();
                write = cache.getCountWrite();
                dataRead = cache.getCountReadData();
                dataWrite = cache.getCountWriteData();
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }

}
