/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import junit.framework.Assert;
import junit.framework.TestCase;

/**
 *
 * @author Cliente
 */
public class BeCacheConnectionTest  extends TestCase{
    
    private static int index = 0;
    
    public void test() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        final BrCacheConnectionPool poll = new BrCacheConnectionPool("localhost", 1044, 2, 1000);

        for(int i=0;i<1;i++){
            Thread th;
            if(false){
                th = new Thread(){
                    public void run(){
                        Random r = new Random();
                        while(true){
                            BrCacheConnection con = null;
                            try{
                                con = poll.getInstance();
                                int rv = index++;
                                //int rv = r.nextInt(200000);
                                String key = String.valueOf(rv)/* + "- INDEX AJBK - "*/;
                                String value = key;
                                con.put(key, 0, value);
                            }
                            catch(Exception e){
                                e.printStackTrace();
                            }
                            finally{
                                if(con != null)
                                    poll.release(con);
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
                            int rv = r.nextInt(10);
                            String key = String.valueOf(rv)/* + "- INDEX AJBK - "*/;
                            String value = key;
                                
                            BrCacheConnection con = null;
                            try{
                                con = poll.getInstance();
                                String val = (String) con.get(key);
                                if(val != null){
                                    //System.out.println(val);
                                    Assert.assertEquals(value, val);
                                }
                            }
                            catch(Throwable e){
                                e.printStackTrace();
                            }
                            finally{
                                if(con != null)
                                    poll.release(con);
                            }
                            
                        }
                    }
                };
                
            }
            
            th.start();
        }
        
        Thread.sleep(999999999);
        
    }
    
    public void test2() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        BrCacheConnectionPool pool = new BrCacheConnectionPool("localhost", 1044, 2, 10);

        BrCacheConnection con = null;
        try{
            con = pool.getInstance();
            String expected = "TESTE \n TESTE \n";
            con.put("tt", 0, expected);
            String value = (String) con.get("tt");
            
            Assert.assertEquals(expected, value);
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
            pool.release(con);
        }
        
        
    }      

    public void test3() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        BrCacheConnectionPool pool = new BrCacheConnectionPool("localhost", 1044, 2, 10);

        BrCacheConnection con = null;
        try{
            con = pool.getInstance();
            String value = (String) con.get("tt");
            Assert.assertNull(value);
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
            pool.release(con);
        }
        
        
    }      
    
}
