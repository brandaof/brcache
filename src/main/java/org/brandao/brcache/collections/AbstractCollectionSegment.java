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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Brandao
 */
abstract class AbstractCollectionSegment<I,T> 
    implements CollectionSegment<I>, 
    CollectionSegmentSwapper<T>, Runnable, Serializable{
    
	private static final long serialVersionUID = 7817500681111470845L;

	private int globalID = 0;
    
    protected Map<Long, Entry<T>> segments;
    
    private transient File path;
    
    private transient boolean hasCreatePath;
    
    private String id;
    
    private int maxCapacity;
    
    private int maxSegmentCapacity;
    
    private double fragmentSize;
    
    private double clearFactor;
    
    protected boolean readOnly;
    
    private volatile long lastSegment;
    
    private Swapper swap;

    private boolean forceSwap;
    
    private volatile Entry<T> firstItem;

    private BlockingQueue<Entry<T>> swapCandidates;
    
    public AbstractCollectionSegment(
            String id, int maxCapacity, double clearFactor,
            double fragmentFactor,
            Swapper swap,
            int quantitySwaperThread) {
        
        this.id                  = id;
        this.fragmentSize        = (int)(maxCapacity * fragmentFactor);
        this.maxCapacity         = maxCapacity;
        this.clearFactor         = clearFactor;
        this.maxSegmentCapacity  = (int)(maxCapacity/fragmentSize);
        this.segments            = new HashMap<Long, Entry<T>>(maxCapacity);
        this.swapCandidates      = new LinkedBlockingQueue<Entry<T>>();
        this.readOnly            = false;
        this.lastSegment         = -1;
        this.swap                = swap;
        this.forceSwap           = true;
        this.swap.setId(this.id);

        Thread[] swapperThreads = new Thread[quantitySwaperThread];
        
        for(int i=0;i<swapperThreads.length;i++){
        	SwapperThread<T> swapperThread = new SwapperThread<T>(swapCandidates, this);
        	swapperThreads[i] = new Thread(swapperThread);
        	swapperThreads[i].start();
        }
        
        Thread clearThread = new Thread(this);
        clearThread.start();
    }
    
    public void run(){
        while(true){
            try{
                Thread.sleep(1000);
            	clearLimit();
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    
    protected synchronized int getNextKey() {
        return globalID++;
    }
    
    protected Object getLock(long segment){
        //return this.locks[segment % this.locks.length];
    	return this;
    }

    protected synchronized void clearLimit() {
    	double i = maxSegmentCapacity * clearFactor;
        double limit = maxSegmentCapacity - i;
        
        if (maxSegmentCapacity > 0 && segments.size() > limit) {
        	int count = 0;
        	Entry<T> item = this.firstItem;
            do{
                if(item != null){
                	this.swapCandidates.add(item);
                	item = item.getNext();
                }
                count++;
            }
            while(count < i && item != null && item != this.firstItem);
        }
        
    }
    
    /*
    protected synchronized void clearLimit() {
        double limit = maxSegmentCapacity - (maxSegmentCapacity * clearFactor);
        if (maxSegmentCapacity > 0 && segments.size() > limit) {
            while(segments.size() > limit){
                Entry<T> item = this.firstItem;
                if(item != null)
                    this.swapOnDisk(item);
            }
        }
    }
    */
    
    protected void clearLimitLength() {
        if (maxSegmentCapacity > 0 && segments.size() > maxSegmentCapacity) {
            double quantity = maxSegmentCapacity * clearFactor;
            clearSegments(quantity);
        }
    }

    private void clearSegments(double quantity){
        int count = 0;
        while(count < quantity){
            Entry<T> item = this.firstItem;
            if(item != null)
                this.swapOnDisk(item);
            count++;
        }
    }

    public void flush(){
    	Entry<T> item;
        while((item = this.firstItem) != null){
            if(item != null)
                this.swapOnDisk(item);
        }
    }
    
    public Entry<T> reload(Entry<T> entity){
        if(entity.isNeedReload())
            return swapOnMemory(entity.getIndex());
        else
            return entity;
    }

    protected void addEntry(long key, Entry<T> item) {
        this.registry(item);
        this.lastSegment = key;
    }

    /*
    protected void addEntry(long key, Entry<T> item) {
        
    	Object lock = this.getLock(key);
    	
        synchronized(lock){
            this.registry(item);
            this.lastSegment = key;
        }
        
    }
    */
    private void registry(Entry<T> item){
        
        if(forceSwap)
            this.clearLimitLength();
        
        segments.put(item.getIndex(), item);
        this.addListedItemOnMemory(item);
        
    }

    private Entry<T> remove(Entry<T> item){
        Entry<T> e = segments.remove(item.getIndex());
        this.removeItemListedOnMemory(item);
        
        item.setItem(null);
        item.setNeedUpdate(false);
        item.setNeedReload(true);
        
        return e;
    }
    
    public void swapOnDisk(Entry<T> item){
    	
    	Object lock = this.getLock(item.getIndex());
    	
        synchronized(lock){
        	
            if(item.isNeedReload())
                return;
        	
            if(!this.readOnly && item.isNeedUpdate())
                this.swap.sendItem(item.getIndex(), item);

            Entry<T> removedItem = this.remove(item);
            
            if(item != removedItem)
                throw new IllegalStateException();
        }
        
    }

    @SuppressWarnings("unchecked")
	public Entry<T> swapOnMemory(long key){

    	if(key > this.lastSegment)
            return null;
        
    	Object lock = this.getLock(key);
    	
        synchronized(lock){
            
            Entry<T> onMemoryEntity = this.segments.get(key);

            if(onMemoryEntity != null)
                return onMemoryEntity;

            if(forceSwap)
                this.clearLimitLength();
            
            Entry<T> entity = (Entry<T>)this.swap.getItem(key);

            if(entity != null){
                segments.put(key, entity);
                this.addListedItemOnMemory(entity);
            }

            return entity;
        }
    }
    
    protected Entry<T> getEntry(long index) {
        
        Entry<T> e = segments.get(index);
        
        if(e == null)
            return swapOnMemory(index);
        else{
        	realocItemListedOnMemory(e);
        	return e;
        }
    }

    /*
    protected Entry<T> getEntry(long index) {
        
        Entry<T> e = segments.get(index);
        
    	Object lock = this.getLock(index);
    	
        synchronized(lock){
            if(e == null)
                return swapOnMemory(index);
            else{
                e = this.reload(e);
            	realocItemListedOnMemory(e);
            	return e;
            }
        }
        
    }
    */
    
    public Map<Long, Entry<T>> getSegments() {
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
        
        if(firstItem == null){
            firstItem = item;
            firstItem.setNext(firstItem);
            firstItem.setBefore(firstItem);
        }
        else{
            Entry<T> lastItem = firstItem.getBefore();

            item.setNext(this.firstItem);
            item.setBefore(lastItem);

            this.firstItem.setBefore(item);
            lastItem.setNext(item);
        }
    }

    private synchronized void removeItemListedOnMemory(Entry<T> item){
    	Entry<T> before      = item.getBefore();
    	Entry<T> next        = item.getNext();
    	
        if(firstItem == item){
            if(firstItem == next)
                    this.firstItem = null;
            else{
                    this.firstItem = next;
                    before.setNext(next);
                    next.setBefore(before);
            }
        }
        else{
            before.setNext(next);
            next.setBefore(before);
        }
        item.setNext(null);
        item.setBefore(null);
    }

    private synchronized void realocItemListedOnMemory(Entry<T> item){
        if(this.firstItem != null && this.firstItem.getBefore() != item){
            this.removeItemListedOnMemory(item);
            this.addListedItemOnMemory(item);
        }
    }
    
}