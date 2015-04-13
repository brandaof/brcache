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
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;

/**
 *
 * @author Brandao
 */
public class BrCacheConnectionTest  extends TestCase{
    
    private static volatile int index = 0;
    
    public void test() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        final BrCacheConnectionPool pool = new BrCacheConnectionPool("localhost", 1044, 1, 1);

        for(int i=0;i<1;i++){
            Thread th;
            if(true){
                th = new Thread(){
                    public void run(){
                        Random r = new Random();
                        while(true){
                            BrCacheConnection con = null;
                            try{
                                con = pool.getConnection();
                                int rv = index++;
                                //int rv = r.nextInt(200000);
                                String key = String.valueOf(rv)/* + "- INDEX AJBK - "*/;
                                String value = key;
                                con.put(key, 0, value );
                                if(rv % 1000 == 0)
                                    System.out.println(rv);
                            }
                            catch(Exception e){
                                e.printStackTrace();
                            }
                            finally{
                                try{
                                    if(con != null)
                                        con.close();
                                }
                                catch(Throwable e){
                                }
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
                                
                            BrCacheConnection con = null;
                            try{
                                con = pool.getConnection();
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
                                try{
                                    if(con != null)
                                        con.close();
                                }
                                catch(Throwable e){
                                }
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
            con = pool.getConnection();
            String expected = "TESTE1";
            String expected2 = "TESTE2";
            con.put("tt", 0, expected);
            Assert.assertEquals(expected, con.get("tt"));
            con.put("tt", 0, expected2);
            Assert.assertEquals(expected2, con.get("tt"));
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
            con.close();
        }
        
        
    }      

    public void test3() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        BrCacheConnectionPool pool = new BrCacheConnectionPool("192.168.0.100", 1044, 2, 10);

        BrCacheConnection con = null;
        try{
            con = pool.getConnection();
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

    public void test4() 
            throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        BrCacheConnectionPool pool = new BrCacheConnectionPool("localhost", 1044, 2, 10);

        for(int i=0;i<10000000;i++){
            BrCacheConnection con = null;
            try{
                con = pool.getConnection();
                con.put(String.valueOf(i),0,i);
                if(i % 100 == 0)
                    System.out.println("added: " + i);
            }
            catch(Exception e){
                e.printStackTrace();
            }
            finally{
                pool.release(con);
            }
        }

        for(int i=0;i<100000;i++){
            BrCacheConnection con = null;
            try{
                if(i % 100 == 0)
                    System.out.println("test: " + i);
                con = pool.getConnection();
                int value = (Integer) con.get(String.valueOf(i));
                Assert.assertEquals(i, value);
            }
            catch(Exception e){
                e.printStackTrace();
            }
            finally{
                pool.release(con);
            }
        }
        
        
    }      

    public void test5() throws IOException, InterruptedException, StorageException, RecoverException {
        
        BrCacheConnectionPool pool = new BrCacheConnectionPool("localhost", 1044, 2, 10);
        BrCacheConnection con = null;
        try{
            con = pool.getConnection();
            con.put("TESTE", 0, "TESTE-VALUE");
            String value = (String) con.get("TESTE");
            Assert.assertEquals("TESTE-VALUE", value);
            con.remove("TESTE");
            value = (String) con.get("TESTE");
            Assert.assertNull(value);
        }
        finally{
            if(con != null)
                con.close();
        }
        
    }      
    
}
