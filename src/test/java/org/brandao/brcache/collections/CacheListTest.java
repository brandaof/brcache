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
import junit.framework.Assert;
import junit.framework.TestCase;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.client.BrCacheClient;

/**
 *
 * @author Brandao
 */
public class CacheListTest extends TestCase{
    
    public void testConnection() throws IOException, CacheException{
        BrCacheClient client = new BrCacheClient("localhost", 1044, 1, 10);
        client.connect();
        
        CacheList.setClient(client);
        
        CacheList<Integer> list = new CacheList<Integer>(100, 0.3, 0.1);
        
        System.out.println("insert");
        for(int i=0;i<1000000;i++){
            list.add(i);
            if(i % 10000 == 0)
                System.out.println("insert " + i);
        }
        
        System.out.println("retrieve");
        
        for(int i=0;i<1000000;i++){
            if(i % 10000 == 0)
                System.out.println("retrieve " + i);
            Assert.assertEquals(i, list.get(i).intValue());
        }
        
    }

    public void testFinalize() throws IOException, CacheException{
        BrCacheClient client = new BrCacheClient("localhost", 1044, 1, 10);
        client.connect();
        
        CacheList.setClient(client);
        
        CacheList<Integer> list = new CacheList<Integer>(100, 0.3, 0.1);
        for(int i=0;i<1000;i++){
            list.add(i);
        }
        
        list.flush();
        
        String id = list.getUniqueId();
        int idLastIndex = (1000/10) - 1;
        Object o = client.get(id + ":" + idLastIndex);
        Assert.assertNotNull(o);
        list.clear();
        o = client.get(id + ":" + idLastIndex);
        Assert.assertNull(o);
        
    }
    
}
