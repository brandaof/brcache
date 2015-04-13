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

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author Brandao
 */
public abstract class AbstractCollectionSegment<I,T> 
    implements CollectionSegment<I>, Serializable{
    
    private int globalID = 0;
    
    protected final ConcurrentMap<Integer, Entry<T>> segments;
    
    private transient File path;
    
    private transient boolean hasCreatePath;
    
    private String id;
    
    private String pathName;
    
    private int maxCapacity;
    
    private int maxSegmentCapacity;
    
    private double fragmentSize;
    
    private double clearFactor;
    
    protected boolean readOnly;
    
    private volatile int lastSegment;
    
    private Swaper<T> swap;

    private final BlockingQueue<Entry<T>> listedItensOnMemory;

    private Object[] locks;
    
    public AbstractCollectionSegment(
            String pathName,
            String id, int maxCapacity, double clearFactor,
            double fragmentFactor) {
        this.id                  = id;
        this.pathName            = pathName;
        this.fragmentSize        = (int)(maxCapacity * fragmentFactor);
        this.maxCapacity         = maxCapacity;
        this.clearFactor         = clearFactor;
        this.maxSegmentCapacity  = (int)(maxCapacity/fragmentSize);
        this.segments            = new ConcurrentHashMap<Integer, Entry<T>>();
        this.readOnly            = false;
        this.lastSegment         = -1;
        this.swap                = new DefaultSwaper<T>(this.id, this.pathName);//new FileSwaper<T>(this.id, this.pathName, (1024*2)*1024);//new DefaultSwaper<T>(this.id, this.pathName);
        this.listedItensOnMemory = new LinkedBlockingQueue<Entry<T>>();
        this.locks               = new Object[1000];
        
        int max = (int)(this.maxSegmentCapacity*0.3 + 1.0);
        
        for(int i=0;i<max;i++){
            Entry<T> e = new Entry<T>(i, false, null);
            e.setOnDisk(false);
            this.segments.put(i, e);
        }
            
        for(int i=0;i<locks.length;i++)
            locks[i] = new Object();
        
        Thread[] clearThread = new Thread[4];
        
        for(int i=0;i<clearThread.length;i++){
            clearThread[i] =
                new Thread(){
                  
                    public void run(){
                        while(true){
                            try{
                                clearLimitLength();
                                Thread.sleep(1000);
                                /*
                                if(listedItensOnMemory.size() < maxSegmentCapacity)
                                    Thread.sleep(1000);
                                
                                Entry<T> segment = listedItensOnMemory.poll();
                                
                                if(segment != null)
                                    swapOnDisk(segment.getIndex(), segment);
                                */
                            }
                            catch(Exception e){
                                e.printStackTrace();
                            }
                        }
                    }
                    
                };
            
            clearThread[i].start();
        }
    }
    
    protected synchronized int getNextKey() {
        return globalID++;
    }
    
    protected Object getLock(int segment){
        return this.locks[segment % this.locks.length];
    }
    
    @SuppressWarnings("unchecked")
    protected void clearLimitLength() {
        if (maxSegmentCapacity > 0 && segments.size() > maxSegmentCapacity) {
            double quantity = maxSegmentCapacity * clearFactor;
            clearSegments(quantity);
        }
    }

    private void clearSegments(double quantity){
        int count = 0;
        while(count < quantity){
            Entry<T> item = this.listedItensOnMemory.poll();
            if(item != null)
                this.swapOnDisk(item.getIndex(), item);
            count++;
        }
    }
    
    private void clearAllSegments(){
        segments.clear();
    }

    public Entry<T> reload(Entry<T> entity){
        if(entity.isOnDisk())
            return swapOnMemory(entity.getIndex());
        else
            return entity;
    }

    protected void addEntry(Integer key, Entry<T> item) {
        
        synchronized(this.getLock(key)){
            segments.put(key, item);
            this.listedItensOnMemory.add(item);
            this.lastSegment = key;
        }
        
    }
    
    private void swapOnDisk(Integer index, Entry<T> item){
        
        synchronized(this.getLock(index)){
            if(!this.readOnly && item.isNeedUpdate())
                this.swap.persistDiskItem(index, item);

            this.listedItensOnMemory.remove(item);

            Entry<T> removedItem = segments.remove(index);

            if(item != removedItem)
                throw new IllegalStateException();

            item.setItem(null);
            item.setNeedUpdate(false);
            item.setOnDisk(true);
        }
        
    }

    private Entry<T> swapOnMemory(Integer key){
        
        if(key > this.lastSegment)
            return null;
        
        synchronized(this.getLock(key)){
            
            Entry<T> onMemoryEntity = this.segments.get(key);

            if(onMemoryEntity != null)
                return onMemoryEntity;

            Entry<T> entity = this.swap.readDiskItem(key);

            if(entity != null){
                segments.put(key, entity);
                this.listedItensOnMemory.add(entity);
            }

            return entity;
        }
        
    }
    
    protected Entry<T> getEntry(Integer index) {
        
        Entry<T> e = segments.get(index);
        
        if(e == null)
            return swapOnMemory(index);
        else
            return e;
    }

    public ConcurrentMap<Integer, Entry<T>> getSegments() {
        return segments;
    }

    public File getPath() {
        return path;
    }

    public void setPath(File path) {
        this.path = path;
    }

    public boolean isHasCreatePath() {
        return hasCreatePath;
    }

    public void setHasCreatePath(boolean hasCreatePath) {
        this.hasCreatePath = hasCreatePath;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

    public void setMaxCapacity(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public int getMaxSegmentCapacity() {
        return maxSegmentCapacity;
    }

    public void setMaxSegmentCapacity(int maxSegmentCapacity) {
        this.maxSegmentCapacity = maxSegmentCapacity;
    }

    public double getFragmentSize() {
        return fragmentSize;
    }

    public void setFragmentSize(float fragmentSize) {
        this.fragmentSize = fragmentSize;
    }

    public double getClearFactor() {
        return clearFactor;
    }

    public void setClearFactor(float clearFactor) {
        this.clearFactor = clearFactor;
    }

    public void clear(){
        this.segments.clear();
        File path = Collections.getPath();
        File swp = new File(path,this.id);
        Collections.deleteDir(swp);
    }

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }
    
    public void flush(){
        this.clearAllSegments();
    }
    
    public void setReadOnly(boolean value){
        this.readOnly = value;
    }
    
    public boolean isReadOnly(){
        return this.readOnly;
    }

}
