
package org.brandao.brcache;

import org.brandao.brcache.client.*;

/**
 *
 * @author Cliente
 */
public class PutTask implements Runnable{

    public static volatile int index = 0;
    
    public static final String text = " lkjfh sldkfjhsdklfjh lskdjfh slakdfh laskdjf hlsdkjfhslakdfjhsldkfjh lf lsakdfjh";
    
    private Cache client;
    
    private int limit;
    
    public PutTask(Cache client, int limit){
        this.client = client;
        this.limit  = limit;
    }
    
    public void run() {
        while(limit < 0 || index < limit){
            try{
                int rv = index++;
                String key = String.valueOf(rv);
                String value = key + text;
                client.putObject(key, 0, value );
                if(rv % 1000 == 0)
                    System.out.println(rv);
            }
            catch(Throwable e){
                e.printStackTrace();
            }
        }
    }
    
}
