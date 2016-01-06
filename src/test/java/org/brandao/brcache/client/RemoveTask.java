
package org.brandao.brcache.client;

import java.util.Random;
import junit.framework.Assert;

/**
 *
 * @author Cliente
 */
public class RemoveTask implements Runnable{

    private BrCacheClient client;
    
    public RemoveTask(BrCacheClient client){
        this.client = client;
    }
    
    public void run() {
        while(true){
            Random r = new Random();
            while(true){
                if(PutTask.index < 5){
                    try{
                        Thread.sleep(1000);
                        continue;
                    }
                    catch(Throwable e){
                    }
                }
                
                int rv = r.nextInt(PutTask.index);
                String key = String.valueOf(rv);
                try{
                    client.remove(key);
                    String val = (String) client.get(key);
                    Assert.assertNull(val);
                }
                catch(Throwable e){
                    e.printStackTrace();
                }
            }
        }
    }
    
}
