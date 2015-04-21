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


package org.brandao.brcache.collections;

import java.io.Serializable;
import org.brandao.brcache.Cache;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.client.BrCacheClient;

/**
 * Faz com que entidades sejam enviadas para um determinado cache.
 * 
 * @author Brandao
 */
public class CacheSwapper<T> 
    implements Swapper<T>, Serializable{

    private String id;
    
    private long maxalive;
    
    private Integer maxIndex;
    
    public CacheSwapper(){
        this.maxIndex = -1;
    }
    
    public void setId(String value) {
        this.id = value;
    }

    @Override
    public void sendItem(Integer index, Entry<T> item) {
        synchronized(this){
            if(this.maxIndex < index)
                this.maxIndex = index;
        }

        if(item.getItem() == null)
            throw new IllegalStateException("invalid item: " + index);
        
        try{
            if(CacheList.getCache() != null)
                this.sendItem(index, item.getItem(), CacheList.getCache());
            else
                this.sendItem(index, item.getItem(), CacheList.getClient());
        }
        catch (StorageException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void sendItem(Integer index, T item, Cache cache) throws StorageException {
        cache.putObject(new String(this.id + ":" + index), this.getMaxalive(), item);
    }

    public void sendItem(Integer index, T item, BrCacheClient client) throws StorageException {
        client.put(new String(this.id + ":" + index), this.getMaxalive(), item);
    }
    
    @Override
    public Entry<T> getItem(Integer index) {
        try{
            T item;
            if(CacheList.getCache() != null)
                item = this.getItem(index, CacheList.getCache());
            else
                item = this.getItem(index, CacheList.getClient());
            
            if(item == null)
                return null;
            
            Entry<T> entry = new Entry<T>(index, false, item);
            entry.setOnDisk(false);
            return entry;
        }
        catch (RecoverException ex) {
            throw new RuntimeException(ex);
        }
    }

    public T getItem(Integer index, Cache cache) throws RecoverException {
        return (T) cache.getObject(new String(this.id + ":" + index));
    }

    public T getItem(Integer index, BrCacheClient client) throws RecoverException {
        return (T) client.get(new String(this.id + ":" + index));
    }
    
    public long getMaxalive() {
        return maxalive;
    }

    public void setMaxalive(long maxalive) {
        this.maxalive = maxalive;
    }

    public void clear() {
        try{
            if(CacheList.getCache() != null)
                this.clear(CacheList.getCache());
            else
                this.clear(CacheList.getClient());
                
        }
        catch (RecoverException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private void clear(Cache cache) throws RecoverException{
        for(int i=0;i<=this.maxIndex;i++){
            cache.remove(new String(this.id + ":" + i));
        }
    }
    
    private void clear(BrCacheClient client) throws RecoverException{
        for(int i=0;i<=this.maxIndex;i++){
            client.remove(new String(this.id + ":" + i));
        }
    }
    
}
