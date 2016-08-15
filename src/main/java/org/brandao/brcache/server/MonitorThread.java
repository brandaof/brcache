/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.server;

import org.brandao.brcache.StreamCache;

/**
 *
 * @author Brandao
 */
class MonitorThread extends Thread{
    
    private boolean run;
    
    private Configuration config;
    
    private StreamCache cache;
    
    public MonitorThread(StreamCache cache, Configuration config){
        this.cache = cache;
        this.config = config;
    }
    
    public void start(){
        this.run = true;
        super.start();
    }

    public void kill(){
        this.run = false;
    }
    
    @Override
    public void run(){
        long lastEntryRead 		= 0;
        long lastEntryWrite 	= 0;
        long lastEntryRemoved 	= 0;
        long readEntry 			= 0;
        long writeEntry 		= 0;
        long removedEntry 		= 0;
        
        long lastRead 			= 0;
        long lastWrite 			= 0;
        long lastRemoved 		= 0;
        long read 				= 0;
        long write 				= 0;
        long removed 			= 0;
        
        while(this.run){
            try{
                Runtime runtime = Runtime.getRuntime();
                long memory = runtime.totalMemory() - runtime.freeMemory();
                this.config.setProperty("write_entry_per_sec",		String.valueOf(writeEntry-lastEntryWrite));
                this.config.setProperty("read_entry_per_sec", 		String.valueOf(readEntry-lastEntryRead));
                this.config.setProperty("removed_entry_per_sec", 	String.valueOf(removedEntry-lastEntryRemoved));
                this.config.setProperty("write_data_per_sec", 		String.valueOf(write-lastWrite));
                this.config.setProperty("read_data_per_sec", 		String.valueOf(read-lastRead));
                this.config.setProperty("removed_data_per_sec", 	String.valueOf(removed-lastRemoved));
                this.config.setProperty("total_memory", 			String.valueOf(runtime.totalMemory()));
                this.config.setProperty("used_memory", 				String.valueOf(memory));
                
                lastEntryRead 		= cache.getCountRead();
                lastEntryWrite 		= cache.getCountWrite();
                lastEntryRemoved 	= cache.getCountRemoved();
                lastRead 			= cache.getCountReadData();
                lastWrite 			= cache.getCountWriteData();
                lastRemoved 		= cache.getCountRemovedData();
                
                Thread.sleep(1000);
                
                readEntry 			= cache.getCountRead();
                writeEntry 			= cache.getCountWrite();
                removedEntry 		= cache.getCountRemoved();
                read 				= cache.getCountReadData();
                write 				= cache.getCountWriteData();
                removed 			= cache.getCountRemovedData();
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    
}
