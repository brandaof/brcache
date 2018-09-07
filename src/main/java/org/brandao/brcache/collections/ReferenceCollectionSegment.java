package org.brandao.brcache.collections;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReferenceCollectionSegment<T> 
	implements ReferenceCollection<T>{

	private static final long serialVersionUID = 4658022218986426713L;

	private static final Empty EMPTY = new Empty();
	
	private BlockingQueue<Long> freeAddress;
	
	private SwapCollection<Object> collection;
	
	private long lastPos;
	
    private boolean deleteOnExit;
	
    private Lock lock;

    public ReferenceCollectionSegment() {
        this(
            HugeArrayList.DEFAULT_MAX_CAPACITY_ELEMENT, 
            HugeArrayList.DEFAULT_CLEAR_FACTOR_ELEMENT, 
            HugeArrayList.DEFAULT_FRAGMENT_FACTOR_ELEMENT,
            null,
            1);
    }

	public ReferenceCollectionSegment(
            int maxCapacityElements,
            double clearFactorElements, 
            double fragmentFactorElements,
            Swapper swap,
            int quantityClearThread) {

    	if(swap == null){
    		throw new NullPointerException("swap");
    	}
		
    	this.freeAddress  = new LinkedBlockingQueue<Long>();
    	this.lastPos      = 0;
    	this.lock         = new ReentrantLock();
        this.deleteOnExit = true;
        this.collection   = 
                    new SegmentedSwapCollectionImp<Object>(
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
			collection.add(new Entry<Object>(index, e));
		}
		catch(Throwable ex){
			this.freeAddress.add(index);
		}
		finally{
			lock.unlock();
		}
			
		return index;
	}

	@SuppressWarnings("unchecked")
	public T set(long reference, T e) {
		Entry<Object> entry = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			entry = collection.getEntry(reference);
			collection.add(new Entry<Object>(reference, e));
		}
		finally{
			lock.unlock();
		}
		T old = entry != null? (T)entry.getItem() : null;
		return old instanceof Empty? null : old;
	}

	@SuppressWarnings("unchecked")
	public T get(long reference) {
		Entry<Object> entry = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			entry = collection.getEntry(reference);
		}
		finally{
			lock.unlock();
		}
		T value = entry != null? (T)entry.getItem() : null;
		return value instanceof Empty? null : value;
	}

	public boolean remove(long reference) {
		Entry<Object> entry = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			entry = collection.getEntry(reference);
			collection.add(new Entry<Object>(reference, EMPTY));
		}
		finally{
			lock.unlock();
		}
		
		Object old = entry != null? entry.getItem() : null;
		return !EMPTY.equals(old);

	}
	
	public boolean replace(long reference, T oldValue, T value) {
		Entry<Object> entry = null;
		Object old;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			entry = collection.getEntry(reference);
			old   = entry != null? entry.getItem() : null;
			if(oldValue.equals(old)){
				collection.add(new Entry<Object>(reference, value));
				return true;
			}
		}
		finally{
			lock.unlock();
		}
		
		return false;
	}

	@SuppressWarnings("unchecked")
	public T replace(long reference, T value) {
		Entry<Object> entry = null;
		Object old = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			entry = collection.getEntry(reference);
			old   = entry != null? entry.getItem() : null;
			if(old != null && !EMPTY.equals(old)){
				collection.add(new Entry<Object>(reference, value));
			}
		}
		finally{
			lock.unlock();
		}
		
		return (T) old;
	}

	@SuppressWarnings("unchecked")
	public T putIfAbsent(long reference, T value) {
		Entry<Object> entry = null;
		Object old = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			entry = collection.getEntry(reference);
			old   = entry != null? entry.getItem() : null;
			if(old == null || EMPTY.equals(old)){
				collection.add(new Entry<Object>(reference, value));
			}
		}
		finally{
			lock.unlock();
		}
		
		return (T) old;
	}

	public boolean remove(long reference, T oldValue) {
		Entry<Object> entry = null;
		Object old = null;
		Lock lock = collection.getGroupLock(reference);
		lock.lock();
		try{
			entry = collection.getEntry(reference);
			old   = entry != null? entry.getItem() : null;
			if(old != null && oldValue.equals(old)){
				collection.add(new Entry<Object>(reference, EMPTY));
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

	private static class Empty implements Serializable{
		
		private static final long serialVersionUID = -881123035360397425L;

		public int hashCode(){
			return 1;
		}
		
		public boolean equals(Object x){
			return x instanceof Empty;
		}
	};
	
}
