package org.brandao.brcache.collections;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Segment<T> {

    protected Map<Long, Entry<T>> data;

    protected Lock lock;
    
    protected Swapper swap;
    
    protected boolean forceSwap;
    
    protected long maxSegmentCapacity;
    
    protected volatile Entry<T> firstItem;
    
    protected boolean readOnly;

    public Segment(Swapper swap,
			boolean forceSwap, long maxSegmentCapacity,	boolean readOnly) {
		super();
		this.data 				= new HashMap<Long, Entry<T>>();
		this.lock 				= new ReentrantLock();
		this.swap 				= swap;
		this.forceSwap 			= forceSwap;
		this.maxSegmentCapacity = maxSegmentCapacity;
		this.firstItem 			= null;
		this.readOnly 			= readOnly;
	}
    
    /* -------- */
    
	public void add(Entry<T> item){
        if(this.forceSwap && this.needSwap()){
            this.swapFirst();
        }
        
        this.data.put(item.getIndex(), item);
        this.addListedItemOnMemory(item);
    }

    protected Entry<T> getEntry(long index) {
        Entry<T> e = this.data.get(index);
        
        if(e == null)
            return swapOnMemory(index);
        else{
        	realocItemListedOnMemory(e);
        	return e;
        }
    }
	
    public Entry<T> remove(Entry<T> item){
        Entry<T> e = this.data.remove(item.getIndex());
        this.removeItemListedOnMemory(item);
        
        //remover do disco também
        
        item.setItem(null);
        item.setNeedUpdate(false);
        item.setNeedReload(true);
        return e;
    }

    public Entry<T> reload(Entry<T> entity){
        if(entity.isNeedReload())
            return swapOnMemory(entity.getIndex());
        else
            return entity;
    }

    public boolean swapNextCandidate(){
    	this.lock.lock();
    	try{
        	Entry<T> item = this.firstItem;
            if(item != null){
                this.swapOnDisk(item);
                return true;
            }
            return false;
    	}
    	finally{
        	lock.unlock();
    	}
    }
    
    public void swap(Entry<T> item){
    	this.lock.lock();
    	try{
    		this.swapOnDisk(item);
    	}
    	finally{
        	lock.unlock();
    	}
    }
    
    /* -------- */
    
    public void swapOnDisk(Entry<T> item){
        if(item.isNeedReload())
            return;
    	
        if(!this.readOnly && item.isNeedUpdate())
            this.swap.sendItem(item.getIndex(), item);

        Entry<T> removedItem = this.remove(item);
        
        if(item != removedItem)
            throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
	public Entry<T> swapOnMemory(long key){
        Entry<T> onMemoryEntity = this.data.get(key);

        if(onMemoryEntity != null)
            return onMemoryEntity;

        if(forceSwap && this.needSwap())
            this.swapFirst();
        
        Entry<T> entity = (Entry<T>)this.swap.getItem(key);

        if(entity != null){
        	this.data.put(key, entity);
            this.addListedItemOnMemory(entity);
        }

        return entity;
    }

    /* -------- */
    
    protected void swapFirst() {
    	Entry<T> item = this.firstItem;
        if(item != null){
            this.swapOnDisk(item);
        }
    }
    
    private void addListedItemOnMemory(Entry<T> item){
        
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

    private void removeItemListedOnMemory(Entry<T> item){
    	Entry<T> before = item.getBefore();
    	Entry<T> next   = item.getNext();
    	
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
    
    private void realocItemListedOnMemory(Entry<T> item){
    	
    	if(item.getBefore() == null && item.getNext() == null)
    		return;
    	
        if(this.firstItem != null && this.firstItem.getBefore() != item){
            this.removeItemListedOnMemory(item);
            this.addListedItemOnMemory(item);
        }
    }
    
    /* -------- */
    
    public void clear(){
    	this.lock.lock();
    	try{
	    	this.firstItem = null;
    		data.clear();
    	}
    	finally{
    		this.lock.unlock();
    	}
    }
    
    protected boolean needSwap(){
    	return maxSegmentCapacity > 0 && this.data.size() > maxSegmentCapacity - 1;    	
    }

	public boolean isReadOnly() {
		return readOnly;
	}

	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	public Lock getLock() {
		return lock;
	}

	public Swapper getSwap() {
		return swap;
	}

	public boolean isForceSwap() {
		return forceSwap;
	}

	public long getMaxSegmentCapacity() {
		return maxSegmentCapacity;
	}
    
}
