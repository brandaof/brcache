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
                    long lastRead = 0;
                    long lastWrite = 0;
                    long read = 0;
                    long write = 0;
                    while(true){
                        try{
                            System.out.println(
                                "write entry: " + (write-lastWrite) + "/sec " +
                                "read entry: " + (read-lastRead) + "/sec");
                            lastRead = cache.getCountRead();
                            lastWrite = cache.getCountWrite();
                            Thread.sleep(1000);
                            read = cache.getCountRead();
                            write = cache.getCountWrite();
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
            if(i % 2 == 0){
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
                            int rv = r.nextInt(index);
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
    
    public void test2() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        final Cache cache = new Cache();

        Thread read =
            new Thread(){
                public void run(){
                    long lastRead = 0;
                    long lastWrite = 0;
                    long read = 0;
                    long write = 0;
                    while(true){
                        try{
                            System.out.println("write: " + (write-lastWrite) + "/sec read: " + (read-lastRead) + "/sec");
                            lastRead = cache.getCountRead();
                            lastWrite = cache.getCountWrite();
                            Thread.sleep(1000);
                            read = cache.getCountRead();
                            write = cache.getCountWrite();
                        }
                        catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                }
            };
        
        read.start();
        
        for(int i=0;i<100000;i++){
            try{
                String expected = "INDEX-" + String.valueOf(i);
                cache.putObject(expected, 0, expected + text);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        
        for(int i=0;i<5;i++){
            Thread th = null;
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
            th.start();
        }
        
        Thread.sleep(999999999);
        
    }    
    
}
