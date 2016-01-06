
package org.brandao.brcache.client;

import java.util.Random;
import junit.framework.Assert;

/**
 *
 * @author Cliente
 */
public class GetTask implements Runnable{

    private BrCacheClient client;
    
    public GetTask(BrCacheClient client){
        this.client = client;
    }
    
    public void run() {
        while(true){
            Random r = new Random();
            while(true){
                if(PutTask.index < 10){
                    try{
                        Thread.sleep(1000);
                        continue;
                    }
                    catch(Throwable e){
                    }
                }
                
                int rv = r.nextInt(PutTask.index-5);
                String key = String.valueOf(rv);
                String value = key + PutTask.text;
                try{
                    String val = (String) client.get(key);
                    Assert.assertEquals(value, val);
                }
                catch(Throwable e){
                    e.printStackTrace();
                }
            }
        }
    }
    
}
