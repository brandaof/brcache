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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author Brandao
 */
public abstract class AbstractCollectionSegment<I,T> 
    implements CollectionSegment<I>, Serializable{
    
    private int globalID = 0;
    
    protected ConcurrentMap<Integer, Entry<T>> segments;
    
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
    
    private Swapper<T> swap;

    private Object[] locks;
    
    private boolean forceSwap;
    
    private NodeEntry firstItem;

    private NodeEntry lastItem;
    
    public AbstractCollectionSegment(
            String pathName,
            String id, int maxCapacity, double clearFactor,
            double fragmentFactor,
            Swapper<T> swap,
            int quantityLock,
            int quantitySwaperThread) {
        
        this.id                  = id;
        this.pathName            = pathName;
        this.fragmentSize        = (int)(maxCapacity * fragmentFactor);
        this.maxCapacity         = maxCapacity;
        this.clearFactor         = clearFactor;
        this.maxSegmentCapacity  = (int)(maxCapacity/fragmentSize);
        this.segments            = new ConcurrentHashMap<Integer, Entry<T>>();
        this.readOnly            = false;
        this.lastSegment         = -1;
        this.swap                = swap;//new DefaultSwaper<T>(this.id, this.pathName);
        this.locks               = new Object[quantityLock];
        this.forceSwap           = false;
        this.swap.setPath(this.pathName);
        this.swap.setId(this.id);

        for(int i=0;i<locks.length;i++)
            locks[i] = new Object();
        
        Thread[] clearThread = new Thread[quantitySwaperThread];
        
        for(int i=0;i<clearThread.length;i++){
            clearThread[i] =
                new Thread(){
                  
                    public void run(){
                        while(true){
                            try{
                                clearLimitLength();
                                Thread.sleep(1000);
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
            Entry<T> item = this.getAndRemoveFirstListedItemOnMemory();
            if(item != null)
                this.swapOnDisk(item.getIndex(), item);
            count++;
        }
    }

    public void flush(){
        Entry<T> item;
        while((item = this.getAndRemoveFirstListedItemOnMemory()) != null){
            if(item != null)
                this.swapOnDisk(item.getIndex(), item);
        }
    }
    
    public Entry<T> reload(Entry<T> entity){
        if(entity.isOnDisk())
            return swapOnMemory(entity.getIndex());
        else
            return entity;
    }

    protected void addEntry(Integer key, Entry<T> item) {
        
        synchronized(this.getLock(key)){
            if(forceSwap)
                this.clearLimitLength();
            
            segments.put(key, item);
            this.addListedItemOnMemory(item);
            this.lastSegment = key;
        }
        
    }
    
    private void swapOnDisk(Integer index, Entry<T> item){
        
        synchronized(this.getLock(index)){
            if(!this.readOnly && item.isNeedUpdate())
                this.swap.persistDiskItem(index, item);

            //this.listedItensOnMemory.remove(item);

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

            if(forceSwap)
                this.clearLimitLength();
            
            Entry<T> entity = this.swap.readDiskItem(key);

            if(entity != null){
                segments.put(key, entity);
                this.addListedItemOnMemory(entity);
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

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }
    
    public void setReadOnly(boolean value){
        this.readOnly = value;
    }
    
    public boolean isReadOnly(){
        return this.readOnly;
    }

    public boolean isForceSwap() {
        return forceSwap;
    }

    public void setForceSwap(boolean forceSwap) {
        this.forceSwap = forceSwap;
    }

    public void clear(){
        this.swap.clear();
    }
    
    private synchronized void addListedItemOnMemory(Entry<T> item){
        
        NodeEntry currentItem = new NodeEntry(item);

        item.setNode(currentItem);
        
        if(firstItem == null){
            firstItem = currentItem;
            lastItem = currentItem;
        }
        else{
            lastItem.setNext(currentItem);
            lastItem = currentItem;
        }
    }

    private synchronized Entry<T> getAndRemoveFirstListedItemOnMemory(){
        
        if(this.firstItem == null)
            return null;
        
        NodeEntry next  = this.firstItem.getNext();
        Entry<T> result = this.firstItem.getEntry();
        
        if(next != null)
            firstItem = next;
        else{
            firstItem = null;
            lastItem = null;
        }
        return result;
    }
    
    private void writeObject(ObjectOutputStream out) throws IOException{
        this.flush();
        out.writeDouble(this.clearFactor);
        out.writeObject(this.firstItem);
        out.writeBoolean(this.forceSwap);
        out.writeDouble(this.fragmentSize);
        out.writeInt(this.globalID);
        out.writeObject(this.id);
        out.writeObject(this.lastItem);
        out.writeInt(this.locks.length);
        out.writeInt(this.lastSegment);
        out.writeInt(this.maxCapacity);
        out.writeInt(this.maxSegmentCapacity);
        out.writeObject(this.pathName);
        out.writeBoolean(this.readOnly);
        out.writeObject(this.segments);
        out.writeObject(this.swap);
    }
    
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{

        this.clearFactor = in.readDouble();
        this.firstItem = (NodeEntry) in.readObject();
        this.forceSwap = in.readBoolean();
        this.fragmentSize = in.readDouble();
        this.globalID = in.readInt();
        this.id = (String) in.readObject();
        this.lastItem = (NodeEntry) in.readObject();
        
        int locksSize = in.readInt();
        this.locks = new Object[locksSize];
        for(int i=0;i<locksSize;i++)
            this.locks[i] = new Object();
        
        this.lastSegment = in.readInt();
        this.maxCapacity = in.readInt();
        this.maxSegmentCapacity = in.readInt();
        this.pathName = (String) in.readObject();
        this.readOnly = in.readBoolean();
        this.segments = (ConcurrentMap<Integer, Entry<T>>) in.readObject();
        this.swap = (Swapper<T>) in.readObject();
    }
    
}
