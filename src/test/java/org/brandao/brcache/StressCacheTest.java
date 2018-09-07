/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.brandao.brcache;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.brandao.brcache.collections.Collections;
import org.brandao.brcache.tx.CacheTransactionManagerImp;

/**
 *
 * @author Brandao
 */
public class StressCacheTest extends TestCase{
    
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
    
	private static final BRCacheConfig config = new TestBRCacheConfig();
    
    public void test() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        Collections.setPath("/mnt/brcache");
        final ConcurrentCache cache = new ConcurrentCache(config)
        	.getTXCache(new CacheTransactionManagerImp("./tx", TimeUnit.SECONDS.toMillis(30)));

        Thread read =
            new Thread(){
                public void run(){
                    long lastRead = 0;
                    long lastWrite = 0;
                    long read = 0;
                    long write = 0;
                    while(true){
                        try{
                            System.out.println("----------------------------------");
                            System.out.println(
                                "write entry: " + (write-lastWrite) + "/sec " +
                                "read entry: " + (read-lastRead) + "/sec");
                            
                            Runtime runtime = Runtime.getRuntime();
                            long memory = runtime.totalMemory() - runtime.freeMemory();
                            System.out.println("Total memory is MB: " + ((runtime.totalMemory()/1024L)/1024L));
                            System.out.println("Used memory is B: " + memory);
                            System.out.println("Used memory is MB: " + ((memory/1024L)/1024L));
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
            Thread th; 
            if(i % 2 == 0){
                th = new Thread(){
                    public void run(){
                        Random r = new Random();
                        while(true){
                            try{
                                int rv = index++;
                                //int rv = r.nextInt(200000);
                                String key = String.valueOf(rv)/* + "- INDEX AJBK - "*/;
                                String value = key + text;
                                	cache.put(key, value, 0, 0);
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
                            String value = key + text;
                                
                            try{
                                String val = (String) cache.get(key);
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
    
    public void test2() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException{
        
        Collections.setPath("/mnt/brcache");
        final ConcurrentCache cache = new ConcurrentCache(config);

        Thread read =
            new Thread(){
                public void run(){
                    long lastRead = 0;
                    long lastWrite = 0;
                    long read = 0;
                    long write = 0;
                    while(true){
                        try{
                            System.out.println("----------------------------------");
                            System.out.println(
                                "write entry: " + (write-lastWrite) + "/sec " +
                                "read entry: " + (read-lastRead) + "/sec");
                            
                            Runtime runtime = Runtime.getRuntime();
                            // Run the garbage collector
                            runtime.gc();
                            // Calculate the used memory
                            long memory = runtime.totalMemory() - runtime.freeMemory();
                            System.out.println("Used memory is bytes: " + memory);
                            System.out.println("Used memory is megabytes: " + ((memory/1024L)/1024L));
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
        
        for(int i=0;i<10000;i++){
            try{
                String key = String.valueOf(i) + "- INDEX AJBK - ";
                String value = key + text;
                cache.put(key, value, 0, 0);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        
        for(int i=0;i<100;i++){
            Thread th = null;
            th = new Thread(){
                public void run(){
                    Random r = new Random();
                    int i = 0;
                    while(true){
                        int rv = r.nextInt(1000);
                        String key = String.valueOf(rv) + "- INDEX AJBK - ";
                        String value = key + text;
                        try{
                            String val = (String) cache.get(key);
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
            th.start();
        }
        
        Thread.sleep(999999999);
        
    }    

}
