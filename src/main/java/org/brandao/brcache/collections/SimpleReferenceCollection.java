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

    private long length;
    
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
				collection.add(length, e);
				long i = length++;
				return i;
			}
			finally{
				lock.unlock();
			}
		}
		else{
			collection.set(index, e);
			return index;
		}
			
	}

	public T set(long reference, T e) {
		return collection.set(reference, e);
	}

	public T get(long reference) {
		return collection.get(reference);
	}

	public boolean remove(long reference) {
		freeAddress.add(reference);
		return collection.set(reference, null) != null;
	}
	
	public boolean replace(long reference, T oldValue, T value) {
		return collection.replace(reference, oldValue, value);
	}

	public T replace(long reference, T value) {
		return collection.replace(reference, value);
	}

	public T putIfAbsent(long reference, T value) {
		return collection.putIfAbsent(reference, value);
	}

	public boolean remove(long reference, T oldValue) {
		freeAddress.add(reference);
		return collection.replace(reference, oldValue, null);
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
