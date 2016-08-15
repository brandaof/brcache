
package org.brandao.brcache;

import org.brandao.brcache.client.*;
import java.util.Random;
import junit.framework.Assert;

/**
 *
 * @author Cliente
 */
public class RemoveTask implements Runnable{

    private NonTransactionalCache client;
    
    public RemoveTask(NonTransactionalCache client){
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
                    String val = (String) client.getObject(key);
                    Assert.assertNull(val);
                }
                catch(Throwable e){
                    e.printStackTrace();
                }
            }
        }
    }
    
}
