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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Brandao
 */
abstract class AbstractCollectionSegment<I,T> 
    implements CollectionSegment<I>, Serializable{
    
	private static final long serialVersionUID = 7817500681111470845L;

	private int globalID = 0;
    
    protected Map<Integer, Entry<T>> segments;
    
    private transient File path;
    
    private transient boolean hasCreatePath;
    
    private String id;
    
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

    public AbstractCollectionSegment(
            String id, int maxCapacity, double clearFactor,
            double fragmentFactor,
            Swapper<T> swap,
            int quantityLock,
            int quantitySwaperThread) {
        
        this.id                  = id;
        this.fragmentSize        = (int)(maxCapacity * fragmentFactor);
        this.maxCapacity         = maxCapacity;
        this.clearFactor         = clearFactor;
        this.maxSegmentCapacity  = (int)(maxCapacity/fragmentSize);
        this.segments            = new LinkedHashMap<Integer, Entry<T>>();//new ConcurrentHashMap<Integer, Entry<T>>();
        this.readOnly            = false;
        this.lastSegment         = -1;
        this.swap                = swap;//new DefaultSwaper<T>(this.id, this.pathName);
        this.locks               = new Object[quantityLock];
        this.forceSwap           = false;
        this.swap.setId(this.id);

        for(int i=0;i<locks.length;i++)
            locks[i] = new Integer(i);
        
        Thread[] clearThread = new Thread[quantitySwaperThread];
        
        for(int i=0;i<clearThread.length;i++){
            clearThread[i] =
                new Thread(){
                  
                    public void run(){
                        while(true){
                            try{
                                clearLimit();
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
        //return this.locks[segment % this.locks.length];
    	return this;
    }

    protected void clearLimit() {
        double limit = maxSegmentCapacity - (maxSegmentCapacity * clearFactor);
        if (maxSegmentCapacity > 0 && segments.size() > limit) {
            System.out.println("start clear");
            while(segments.size() > limit){
                Entry<T> index = this.getAndRemoveFirstListedItemOnMemory();
                if(index != null){
                    this.swapOnDisk(index.getIndex(), index);
                }
            }
            System.out.println("end clear");
        }
    }
    
    
    protected void clearLimitLength() {
        if (maxSegmentCapacity > 0 && segments.size() > maxSegmentCapacity) {
            double quantity = maxSegmentCapacity * clearFactor;
            System.out.println("clear: " + quantity);
            clearSegments(quantity);
        }
    }

    private void clearSegments(double quantity){
        int count = 0;
        while(count < quantity){
        	Entry<T> index = this.getAndRemoveFirstListedItemOnMemory();
            //Entry<T> item = this.segments.get(index);
            if(index != null){
                this.swapOnDisk(index.getIndex(), index);
            }
            count++;
        }
    }

    public void flush(){
    	Entry<T> index;
        while((index = this.getAndRemoveFirstListedItemOnMemory()) != null){
            //Entry<T> item = this.segments.get(index);
            if(index != null){
                this.swapOnDisk(index.getIndex(), index);
            }
        }
        this.firstItem = null;
    }
    
    public Entry<T> reload(Entry<T> entity){
        if(entity.isNeedReload())
            return swapOnMemory(entity.getIndex());
        else
            return entity;
    }

    protected void addEntry(Integer key, Entry<T> item) {
        
    	Object lock = this.getLock(key);
    	
        synchronized(lock){
            if(forceSwap)
                this.clearLimitLength();
            
            segments.put(key, item);
            this.addListedItemOnMemory(item);
            this.lastSegment = key;
        }
        
    }
    
    private void swapOnDisk(Integer index, Entry<T> item){
    	
    	Object lock = this.getLock(index);
    	
        synchronized(lock){
        	
        	if(item.isNeedReload())
        		return;
        	
            if(!this.readOnly && item.isNeedUpdate())
                this.swap.sendItem(index, item);

            //this.listedItensOnMemory.remove(item);

            Entry<T> removedItem = segments.remove(index);

            if(item != removedItem)
                throw new IllegalStateException();

            item.setItem(null);
            item.setNeedUpdate(false);
            item.setNeedReload(true);
        }
        
    }

    private Entry<T> swapOnMemory(Integer key){

    	if(key > this.lastSegment)
            return null;
        
    	Object lock = this.getLock(key);
    	
        synchronized(lock){
            
            Entry<T> onMemoryEntity = this.segments.get(key);

            if(onMemoryEntity != null)
                return onMemoryEntity;

            if(forceSwap)
                this.clearLimitLength();
            
            Entry<T> entity = this.swap.getItem(key);

            if(entity != null){
                segments.put(key, entity);
                this.addListedItemOnMemory(entity);
            }

            return entity;
        }
    }
    
    protected Entry<T> getEntry(Integer index) {
        
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

    public Map<Integer, Entry<T>> getSegments() {
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
        
        NodeEntry currentItem = new NodeEntry(item);
        item.setNode(currentItem);
        
        if(firstItem == null){
            firstItem = currentItem;
            firstItem.setNext(firstItem);
            firstItem.setBefore(firstItem);
        }
        else{
        	NodeEntry lastItem = firstItem.getBefore();
        	
        	currentItem.setNext(this.firstItem);
        	currentItem.setBefore(lastItem);
        	
        	this.firstItem.setBefore(currentItem);
        	lastItem.setNext(currentItem);
        }
    }

    private synchronized void removeItemListedOnMemory(Entry<T> item){
    	NodeEntry currentItem = item.getNode();
    	NodeEntry before      = currentItem.getBefore();
    	NodeEntry next        = currentItem.getNext();
    	
        if(firstItem == currentItem){
        	this.firstItem = next;
        	before.setNext(next);
        	next.setBefore(before);
        }
        else{
        	before.setNext(next);
        	next.setBefore(before);
        }
    }

    private synchronized void realocItemListedOnMemory(Entry<T> item){
    	NodeEntry currentItem = item.getNode();
    	NodeEntry next        = currentItem.getNext();
    	
        if(firstItem == currentItem){
        	this.firstItem = next;
        	NodeEntry lastItem = firstItem.getBefore();
        	
        	currentItem.setNext(this.firstItem);
        	currentItem.setBefore(lastItem);
        	
        	this.firstItem.setBefore(currentItem);
        	lastItem.setNext(currentItem);
        }
        else{
        	NodeEntry lastItem = firstItem.getBefore();
        	
        	currentItem.setNext(this.firstItem);
        	currentItem.setBefore(lastItem);
        	
        	this.firstItem.setBefore(currentItem);
        	lastItem.setNext(currentItem);
        }
    }
    
    private synchronized Entry<T> getAndRemoveFirstListedItemOnMemory(){
        
        if(this.firstItem == null)
            return null;
        
        Entry<T> item = this.firstItem.getIndex();
        this.removeItemListedOnMemory(item);
        return item;
    }
    
}
