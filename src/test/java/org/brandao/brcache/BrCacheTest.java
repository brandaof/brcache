
package org.brandao.brcache;

import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.Assert;
import junit.framework.TestCase;

/**
 *
 * @author Brandao
 */
public class BrCacheTest extends TestCase{
    
    public void testInsertOnMemory() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException, CacheException{

        Cache cache = new Cache();
        
        Thread th = new Thread(new PutTask(cache, 100));
        th.start();

        th = new Thread(new GetTask(cache));
        th.start();
        
        Thread.sleep(999999999);
        
    }        

    public void testOverrideMemory() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException, CacheException{
        
        Cache cache = new Cache();
        
        PutTask o = new PutTask(cache, 100);
        o.run();

        Thread th = new Thread(new OverrideTask(cache));
        th.start();
        
        Thread.sleep(999999999);
        
    }        

    public void testOverrideSwap() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException, CacheException{
        
        Cache cache = new Cache();
        
        PutTask o = new PutTask(cache, 100000);
        o.run();

        Thread th = new Thread(new OverrideTask(cache));
        th.start();
        
        Thread.sleep(999999999);
        
    }        
    
    public void testPut() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException, CacheException{
        
        Cache cache = new Cache();
        
        for(int i=0;i<10;i++){
            Thread th = new Thread(new PutTask(cache,-1));
            th.start();
        }

        for(int i=0;i<1;i++){
            Thread th = new Thread(new GetTask(cache));
            th.start();
        }
        
        Thread.sleep(999999999);
        
    }    
    
}
