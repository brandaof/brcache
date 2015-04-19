/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.client.BrCacheClient;
import org.brandao.brcache.collections.CacheList;

/**
 *
 * @author Brandao
 */
public class CompressStreamFactoryTest extends TestCase{
    
    public void testCompress() throws IOException, CacheException, InterruptedException{
        Configuration config = new Configuration();
        config.setProperty("compress_stream", "true");
        config.setProperty("port", "1044");
        
        final BrCacheServer server = new BrCacheServer(config);
        
        Thread serverThread = new Thread(){
          
            public void run(){
                try{
                    server.start();
                }
                catch(IOException e){
                    e.printStackTrace();
                }
            }
        };
        
        serverThread.start();
        
        Thread.sleep(2000);
        
        Assert
            .assertTrue(server.getStreamFactory() instanceof CompressStreamFactory);

        
        CompressStreamFactory streamFactory = new CompressStreamFactory();
        
        BrCacheClient client = new BrCacheClient("localhost", 1044, 1, 10, streamFactory);
        
        client.connect();
        
        CacheList.setClient(client);
        
        CacheList<Integer> list = new CacheList<Integer>(100, 0.3, 0.1);
        
        System.out.println("insert");
        for(int i=0;i<100000;i++){
            list.add(i);
            //if(i % 1000 == 0)
                System.out.println("insert " + i);
        }
        
        System.out.println("retrieve");
        
        for(int i=0;i<100000;i++){
            //if(i % 1000 == 0)
                System.out.println("retrieve " + i);
            Assert.assertEquals(i, list.get(i).intValue());
        }
        
    }
}
