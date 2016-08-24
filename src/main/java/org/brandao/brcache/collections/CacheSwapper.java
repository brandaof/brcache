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

/**
 * Faz com que entidades sejam enviadas para um determinado cache.
 * 
 * @author Brandao
 */
public class CacheSwapper 
    implements Swapper, Serializable{

	private static final long serialVersionUID = -1342760584177753822L;

	private String id;
    
    private long maxalive;
    
    private long maxIndex;
    
    private Cache cache;
    
    public CacheSwapper(Cache cache){
        this.maxIndex = -1;
        this.cache = cache;
    }
    
    public void setId(String value) {
        this.id = value;
    }

    public void sendItem(long index, Entry<?> item) {
        synchronized(this){
            if(this.maxIndex < index)
                this.maxIndex = index;
        }

        if(item.getItem() == null)
            throw new IllegalStateException("invalid item: " + index);
        
        try{
            this.sendItem(index, item.getItem());
        }
        catch (StorageException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void sendItem(long index, Object item) throws StorageException {
        cache.put(this.id + ":" + index, item, this.getMaxalive());
    }

    public Entry<?> getItem(long index) {
        try{
            Object item = cache.get(this.id + ":" + index);
            
            if(item == null)
                return null;
            
            Entry<?> entry = new Entry<Object>(index, false, item);
            entry.setNeedReload(false);
            return entry;
        }
        catch (RecoverException ex) {
            throw new RuntimeException(ex);
        }
    }

    public long getMaxalive() {
        return maxalive;
    }

    public void setMaxalive(long maxalive) {
        this.maxalive = maxalive;
    }

    public void clear() {
        try{
            this.clear(this.cache);
        }
        catch (Throwable ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private void clear(Cache cache) throws StorageException{
        for(int i=0;i<=this.maxIndex;i++){
            cache.remove(this.id + ":" + i);
        }
    }
    
}
