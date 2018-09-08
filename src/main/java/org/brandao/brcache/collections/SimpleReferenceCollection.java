package org.brandao.brcache.collections;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleReferenceCollection<T> 
	implements ReferenceCollection<T>{

	private static final long serialVersionUID = 4658022218986426713L;

	private BlockingQueue<Long> freeAddress;
	
	private SwapCollection<T> collection;
	
	private long lastPos;
	
    private boolean deleteOnExit;
	
    private Lock lock;

    public SimpleReferenceCollection() {
        this(
            HugeArrayList.DEFAULT_MAX_CAPACITY_ELEMENT, 
            HugeArrayList.DEFAULT_CLEAR_FACTOR_ELEMENT, 
            HugeArrayList.DEFAULT_FRAGMENT_FACTOR_ELEMENT,
            null,
            1);
    }

	public SimpleReferenceCollection(
            int maxCapacityElements,
            double clearFactorElements, 
            double fragmentFactorElements,
            Swapper<T> swap,
            int quantityClearThread) {

    	if(swap == null){
    		throw new NullPointerException("swap");
    	}
		
    	this.freeAddress  = new LinkedBlockingQueue<Long>();
    	this.lastPos      = 0;
    	this.lock         = new ReentrantLock();
        this.deleteOnExit = true;
        this.collection   = 
                    new SegmentedSwapCollectionImp<T>(
                        maxCapacityElements, 
                        clearFactorElements, 
                        fragmentFactorElements,
                        swap,
                        quantityClearThread);
    }
    
	public long insert(T e) {
		
		Long index = this.freeAddress.poll();
		
		if(index == null){
			lock.lock();
			try{
				index = this.lastPos++;
			}
			finally{
				lock.unlock();
			}
		}
		
			
		Lock lock = collection.getGroupLock(index);
		lock.lock();
		try{
			collection.add(new Entry<T>(index, e));
		}
		catch(Throwable ex){
			this.freeAddress.add(index);
			throw new IllegalStateException(ex);
		}
		finally{
			lock.unlock();
		}
			
		return index;
	}

	public T set(long reference, T e) {
		T old = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			Entry<T> entry = collection.getEntry(reference);
			if(entry != null){
				old = entry.getItem();
				entry.setItem(e);
				entry.setNeedUpdate(true);
			}
			else{
				throw new IllegalStateException();
			}
		}
		finally{
			lock.unlock();
		}
		
		return old;
	}

	public T get(long reference) {
		T v = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			Entry<T> entry = collection.getEntry(reference);
			v = entry == null? null : entry.getItem();
		}
		finally{
			lock.unlock();
		}
		return v;
	}

	public boolean remove(long reference) {
		T v = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			Entry<T> entry = collection.getEntry(reference);
			if(entry != null){
				v = entry.getItem();
				entry.setItem(null);
				entry.setNeedUpdate(true);
				freeAddress.add(reference);
			}
		}
		finally{
			lock.unlock();
		}
		
		return v != null;

	}
	
	public boolean replace(long reference, T oldValue, T value) {
		Object old;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			Entry<T> entry = collection.getEntry(reference);
			old = entry != null? entry.getItem() : null;
			if(oldValue.equals(old)){
				entry.setItem(value);
				entry.setNeedUpdate(true);
				return true;
			}
		}
		finally{
			lock.unlock();
		}
		
		return false;
	}

	public T replace(long reference, T value) {
		T v = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			Entry<T> entry = collection.getEntry(reference);
			v = entry != null? entry.getItem() : null;
			if(v != null){
				entry.setItem(value);
				entry.setNeedUpdate(true);
			}
		}
		finally{
			lock.unlock();
		}
		
		return v;
	}

	public T putIfAbsent(long reference, T value) {
		T v = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			Entry<T> entry = collection.getEntry(reference);
			v = entry != null? entry.getItem() : null;
			if(v == null){
				if(entry != null){
					entry.setItem(value);
					entry.setNeedUpdate(true);
				}
				else{
					collection.add(new Entry<T>(reference, value));
				}
			}
		}
		finally{
			lock.unlock();
		}
		
		return v;
	}

	public boolean remove(long reference, T oldValue) {
		T v = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			Entry<T> entry = collection.getEntry(reference);
			v = entry != null? entry.getItem() : null;
			if(oldValue.equals(v)){
				if(entry != null){
					entry.setItem(null);
					entry.setNeedUpdate(true);
					freeAddress.add(reference);
				}
				return true;
			}
		}
		finally{
			lock.unlock();
		}
		
		return false;
	}
	
    public void setDeleteOnExit(boolean value){
    	this.deleteOnExit = value;
    }

    public boolean isDeleteOnExit(){
    	return this.deleteOnExit;
    }
	
    public int size(){
    	return (int)this.length();
    }
    
    public long length(){
    	return this.lastPos;
    }

    public boolean isEmpty(){
    	return this.size() == 0;
    }
    
    public boolean contains(Object value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void clear() {
		collection.clear();
    }

    public void destroy(){
    	collection.destroy();
    }
    
    public void flush(){
    	collection.flush();
    }

	public void setReadOnly(boolean value) {
		collection.setReadOnly(value);
	}

	public boolean isReadOnly() {
    	return this.collection.isReadOnly();
	}
	
}
