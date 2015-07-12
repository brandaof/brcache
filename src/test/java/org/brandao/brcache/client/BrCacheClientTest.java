
package org.brandao.brcache.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.brandao.brcache.CacheException;

/**
 *
 * @author Brandao
 */
public class BrCacheClientTest extends TestCase{
    
    private static volatile int index = 0;
    
    public void test() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException, CacheException{
        
        final BrCacheClient client = new BrCacheClient("localhost", 1044, 1, 100);
        client.connect();
        
        for(int i=0;i<10;i++){
            Thread th;
            if(i%2==0){
                th = new Thread(){
                    public void run(){
                        Random r = new Random();
                        while(true){
                            try{
                                int rv = index++;
                                //int rv = r.nextInt(200000);
                                String key = String.valueOf(rv)/* + "- INDEX AJBK - "*/;
                                String value = key;
                                client.put(key, 0, value );
                                if(rv % 1000 == 0)
                                    System.out.println(rv);
                            }
                            catch(Exception e){
                                e.printStackTrace();
                            }
                            index++;
                        }
                    }
                };
            }
            else
            /*if(i % 99 == 0){
                th = new Thread(){
                    public void run(){
                        Random r = new Random();
                        while(true){
                            int rv = r.nextInt(900000);
                            String expected = "INDEX-" + String.valueOf(rv);
                            try{
                                cache.remove(expected);
                            }
                            catch(Throwable e){
                                e.printStackTrace();
                            }
                        }
                    }
                };
            }
            else*/{
                th = new Thread(){
                    public void run(){
                        Random r = new Random();
                        while(true){
                            //int rv = r.nextInt(index <= 0? 1000000 : index);
                            int rv = r.nextInt(index);
                            String key = String.valueOf(rv)/* + "- INDEX AJBK - "*/;
                            String value = key;
                                
                            try{
                                String val = (String) client.get(key);
                                if(val != null){
                                    //System.out.println(val);
                                    Assert.assertEquals(value, val);
                                }
                            }
                            catch(Throwable e){
                                e.printStackTrace();
                            }
                        }
                    }
                };
                
            }
            
            th.start();
        }
        
        Thread.sleep(999999999);
        
    }    
}
