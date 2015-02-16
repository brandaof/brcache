/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.brandao.brcache;

import java.io.*;
import java.util.Random;
import junit.framework.Assert;
import junit.framework.TestCase;

/**
 *
 * @author Brandao
 */
public class CacheTest extends TestCase{
    
    private static String text = 
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh";
         
    private static int index = 0;
    
    private static int count = 0;
    
    public void test() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        final Cache cache = new Cache();

        Thread read =
            new Thread(){
                public void run(){
                    while(true){
                        try{
                            System.out.println("write: " + cache.getWritePerSec() + "/sec read: " + cache.getReadPerSec() + "/sec");
                            //System.out.println("\"" + count++ +   "\",\"" + cache.getWritePerSec() + "\",\"" + cache.getReadPerSec() + "\"");
                            Thread.sleep(1200);
                        }
                        catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                }
            };
        
        read.start();
        
        for(int i=0;i<100;i++){
            Thread th = null;
            if(i % 10 == 0){
                th = new Thread(){
                    public void run(){
                        int rv = index++;
                        while(rv<1000000){
                            String expected = "INDEX-" + String.valueOf(rv);
                            //System.out.println(rv);
                            try{
                                cache.putObject(expected, 0, expected + text);
                            }
                            catch(Exception e){
                                e.printStackTrace();
                            }
                            rv = index++;
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
                            int rv = r.nextInt(900000);
                            String expected = "INDEX-" + String.valueOf(rv);
                            try{
                                String val = (String) cache.getObject(expected);
                                if(val != null){
                                    //System.out.println(val);
                                    Assert.assertEquals(expected + text, val);
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
