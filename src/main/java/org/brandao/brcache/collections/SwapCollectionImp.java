package org.brandao.brcache.collections;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SwapCollectionImp<T> 
	implements SwapCollection<T>{

	private final long id;
	
    protected Map<Long, Entry<T>> data;

    protected Lock lock;
    
    protected Swapper<T> swap;
    
    protected boolean forceSwap;
    
    protected long maxSegmentCapacity;
    
    protected volatile Entry<T> firstItem;
    
    protected boolean readOnly;

    public SwapCollectionImp(Swapper<T> swap, boolean forceSwap, 
    		long maxSegmentCapacity, boolean readOnly) {
    	this.id                 = getNextUniqueID();
		this.data 				= new HashMap<Long, Entry<T>>();
		this.lock 				= new ReentrantLock();
		this.swap 				= swap;
		this.forceSwap 			= forceSwap;
		this.maxSegmentCapacity = maxSegmentCapacity;
		this.firstItem 			= null;
		this.readOnly 			= readOnly;
	}
    
    public long getId(){
    	return id;
    }

	public void add(long index, T item){
		lock.lock();
		try{
			Entry<T> e = new Entry<T>(index, item);
			add(e);
		}
		finally{
			lock.unlock();
		}
	}
    
	public T set(long index, T item){
		lock.lock();
		try{
			Entry<T> e = getEntry(index);
			T old = e.getItem();
			e.setItem(item);
			e.setNeedUpdate(true);
			return old;
		}
		finally{
			lock.unlock();
		}
	}

	public T get(long index){
		lock.lock();
		try{
			Entry<T> e = getEntry(index);
			return e.getItem();
		}
		finally{
			lock.unlock();
		}
	}

	public boolean replace(long index, T oldValue, T item){
		lock.lock();
		try{
			Entry<T> e = getEntry(index);
			T old = e.getItem();
			if(oldValue.equals(old)){
				e.setItem(item);
				e.setNeedUpdate(true);
				return true;
			}
			return false;
		}
		finally{
			lock.unlock();
		}
	}

	public T replace(long index, T item){
		lock.lock();
		try{
			Entry<T> e = getEntry(index);
			T old = e.getItem();
			e.setItem(item);
			e.setNeedUpdate(true);
			return old;
		}
		finally{
			lock.unlock();
		}
	}

	public T putIfAbsent(long index, T item){
		lock.lock();
		try{
			Entry<T> e = getEntry(index);
			T old = e.getItem();
			if(old == null){
				e.setItem(item);
				e.setNeedUpdate(true);
				return null;
			}
			return old;
		}
		finally{
			lock.unlock();
		}
	}
	
	protected void add(Entry<T> item){
        if(this.forceSwap && this.needSwap()){
            this.swapFirst();
        }
        
        if(data.put(item.getIndex(), item) != null){
        	throw new IllegalStateException();
        }
        
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
	
    protected Entry<T> remove(Entry<T> item){
        Entry<T> e = this.data.remove(item.getIndex());
        this.removeItemListedOnMemory(item);
        
        //remover do disco também
        
        item.setItem(null);
        item.setNeedUpdate(false);
        item.setNeedReload(true);
        return e;
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
    
    protected void swapOnDisk(Entry<T> item){

        if(item.isNeedReload())
            return;
    	
        if(!this.readOnly && item.isNeedUpdate())
            this.swap.sendItem(item.getIndex(), item);

        Entry<T> removedItem = this.remove(item);
        
        if(item != removedItem)
            throw new IllegalStateException();
    }

	protected Entry<T> swapOnMemory(long key){
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

    protected void swapFirst() {
    	Entry<T> item = this.firstItem;
        if(item != null){
            this.swapOnDisk(item);
        }
    }
    
    protected void addListedItemOnMemory(Entry<T> item){
        
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

    protected void removeItemListedOnMemory(Entry<T> item){
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
    
    protected void realocItemListedOnMemory(Entry<T> item){
    	
    	if(item.getBefore() == null && item.getNext() == null)
    		return;
    	
        if(this.firstItem != null && this.firstItem.getBefore() != item){
            this.removeItemListedOnMemory(item);
            this.addListedItemOnMemory(item);
        }
    }
    
    public void clear(){
    	this.lock.lock();
    	try{
	    	this.firstItem = null;
    		data.clear();
    		swap.clear();
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

	public Swapper<T> getSwap() {
		return swap;
	}

	public boolean isForceSwap() {
		return forceSwap;
	}

	public long getMaxSegmentCapacity() {
		return maxSegmentCapacity;
	}
    
	private static long uniqueID = 0;

	private static final long getNextUniqueID(){
		long value = uniqueID++;
		return value;
	}

	public int getNumberOfGroups() {
		return 1;
	}

	public Lock getGroupLock(long index) {
		return lock;
	}

	public void flush() {
		while(swapNextCandidate());		
	}

	public void destroy() {
    	this.lock.lock();
    	try{
	    	this.firstItem = null;
    		data.clear();
    		swap.destroy();
    	}
    	finally{
    		this.lock.unlock();
    	}
	}

	public void setForceSwap(boolean value) {
		this.forceSwap = value;
	}

}
