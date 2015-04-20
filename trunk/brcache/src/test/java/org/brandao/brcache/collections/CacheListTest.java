/*
 * Copyright 2015 Cliente.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.collections;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.client.BrCacheClient;

/**
 *
 * @author Brandao
 */
public class CacheListTest extends TestCase{

    private static final Object lock = new Object();
    
    public CacheListTest() throws InterruptedException{
    }
    
    
    public void testInsertAndRetrieve() throws IOException, CacheException{
        BrCacheClient client = new BrCacheClient("localhost", 1044, 1, 10);
        client.connect();
        
        CacheList.setClient(client);
        
        CacheList<Integer> list = new CacheList<Integer>(100, 0.3, 0.1);
        
        System.out.println("insert");
        for(int i=0;i<100000;i++){
            list.add(i);
            if(i % 1000 == 0)
                System.out.println("insert " + i);
        }
        
        System.out.println("retrieve");
        
        for(int i=0;i<100000;i++){
            if(i % 1000 == 0)
                System.out.println("retrieve " + i);
            Assert.assertEquals(i, list.get(i).intValue());
        }
        
    }

    public void testSerialize() throws IOException, CacheException {
        BrCacheClient client = new BrCacheClient("localhost", 1044, 1, 10);
        client.connect();
        
        CacheList.setClient(client);
        
        CacheList<Integer> list = new CacheList<Integer>(100, 0.3, 0.1);
        
        for(int i=0;i<1000;i++){
            list.add(i);
        }

        client.put("cache:teste", 0, list);
        
        int totalSegments = (1000/10) - 1;

        String id = list.getUniqueId();
        
        for(int i=0;i<totalSegments;i++){
            Object o = client.get(id + ":" + i);
            Assert.assertNotNull(o);
        }
        
        list = (CacheList<Integer>)client.get("cache:teste");

        for(int i=0;i<1000;i++){
            Assert.assertEquals(i, list.get(i).intValue());
        }
        
        
    }

    public void testConcurrentAccess() throws CacheException, IOException, InterruptedException{
        final BrCacheClient client = new BrCacheClient("localhost", 1044, 1, 10);
        client.connect();
        final List<Throwable> erros = new ArrayList<Throwable>();
        
        CacheList.setClient(client);
        
        final CacheList<Integer> list = new CacheList<Integer>(100, 0.3, 0.1);
        final Random r = new Random();
        for(int i=0;i<350;i++){
            Thread insertTask = new Thread(){
                public void run(){
                    try{
                        while(true){
                            synchronized(lock){
                                list.add(r.nextInt(1000000));
                            }
                            Thread.sleep(800);
                        }
                    }
                    catch(Exception e){
                        erros.add(e);
                        e.printStackTrace();
                    }
                }
            };
            insertTask.start();
        }
        
        Thread sendCache = new Thread(){
                public void run(){
                    try{
                        while(true){
                            synchronized(lock){
                                client.put("cache:teste", 0, list);
                            }
                            Thread.sleep(1000);
                        }
                    }
                    catch(Exception e){
                        erros.add(e);
                        e.printStackTrace();
                    }
                }
            };
            sendCache.start();
            
        Thread readCache = new Thread(){
                public void run(){
                    CacheList<Integer> tmp;
                    try{
                        while(true){
                            tmp = (CacheList<Integer>) client.get("cache:teste");
                            if(tmp != null){
                                for(int i=0;i<tmp.size();i++){
                                    tmp.get(i);
                                }
                            }
                            Thread.sleep(1000);
                        }
                    }
                    catch(Exception e){
                        erros.add(e);
                        e.printStackTrace();
                    }
                }
            };
            readCache.start();
            
            Thread.sleep(60000);

            System.out.println("end -------------------------------------");
            
            if(!erros.isEmpty()){
                for(Throwable e: erros)
                    e.printStackTrace();
                Assert.fail();
            }
    }
}
