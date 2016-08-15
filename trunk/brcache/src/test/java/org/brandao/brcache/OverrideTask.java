
package org.brandao.brcache;

import org.brandao.brcache.client.*;
import java.util.Random;
import junit.framework.Assert;

/**
 *
 * @author Cliente
 */
public class OverrideTask implements Runnable{

    private NonTransactionalCache client;
    
    public OverrideTask(NonTransactionalCache client){
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
                
                try{
                    String key = String.valueOf(rv) + PutTask.suffixKey;
                    String value = key + PutTask.text;
                    
                    String val = (String) client.getObject(key);
                    Assert.assertEquals(value, val);
                    
                    value = "A" + key + PutTask.text;
                    client.putObject(key, 0, value);
                    
                    val = (String) client.getObject(key);
                    Assert.assertEquals(value, val);
                    
                    client.putObject(key, 0, key + PutTask.text);
                }
                catch(Throwable e){
                    e.printStackTrace();
                }
            }
        }
    }
    
}
