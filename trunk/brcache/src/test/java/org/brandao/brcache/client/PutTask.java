
package org.brandao.brcache.client;

/**
 *
 * @author Cliente
 */
public class PutTask implements Runnable{

    public static volatile int index = 0;
    
    public static final String text = " lkjfh sldkfjhsdklfjh lskdjfh slakdfh laskdjf hlsdkjfhslakdfjhsldkfjh lf lsakdfjh";
    
    private BrCacheClient client;
    
    private int limit;
    
    public PutTask(BrCacheClient client, int limit){
        this.client = client;
        this.limit  = limit;
    }
    
    public void run() {
        while(limit < 0 || index < limit){
            try{
                int rv = index++;
                String key = String.valueOf(rv);
                String value = key + text;
                client.put(key, 0, value );
                if(rv % 1000 == 0)
                    System.out.println(rv);
            }
            catch(Throwable e){
                e.printStackTrace();
            }
        }
    }
    
}
