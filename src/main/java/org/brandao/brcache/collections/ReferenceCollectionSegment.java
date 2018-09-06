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
	
	private SegmentedCollection<Object> collection;
	
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
                    new SegmentedCollectionImp<Object>(
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
			
			
			long segment = (long)(index / collection.getFragmentSize());
			long offset  = (long)(index % collection.getFragmentSize());
	
			try{
				collection.putEntity(segment, (int)offset, e);
			}
			catch(Throwable ex){
				this.freeAddress.add(index);
			}
			
		}
		else{
			long segment = (long)(index / collection.getFragmentSize());
			long offset  = (long)(index % collection.getFragmentSize());
			
			try{
				collection.putEntity(segment, (int)offset, e);
			}
			catch(Throwable ex){
				this.freeAddress.add(index);
			}
			
		}
			
		return index;
	}

	@SuppressWarnings("unchecked")
	public T set(long reference, T e) {
		long segment    = (long)(reference / collection.getFragmentSize());
		long offset     = (long)(reference % collection.getFragmentSize());
		Object o = collection.setEntity(segment, (int)offset, (T)e);
		return o instanceof Empty? null : (T)o;
	}

	@SuppressWarnings("unchecked")
	public T get(long reference) {
		long segment    = (long)(reference / collection.getFragmentSize());
		long offset     = (long)(reference % collection.getFragmentSize());
		Object o = collection.getEntity(segment, (int)offset);
		return o instanceof Empty? null : (T)o;
	}

	public boolean remove(long reference) {
		long segment    = (long)(reference / collection.getFragmentSize());
		long offset     = (long)(reference % collection.getFragmentSize());
		
		if(!EMPTY.equals(collection.replaceEntity(segment, (int)offset, EMPTY))){
			this.freeAddress.add(reference);
			return true;
		}
		else{
			return false;
		}
	}
	
	public boolean replace(long reference, T oldValue, T value) {
		long segment    = (long)(reference / collection.getFragmentSize());
		long offset     = (long)(reference % collection.getFragmentSize());
		return collection.replaceEntity(segment, (int)offset, oldValue, value);
	}

	@SuppressWarnings("unchecked")
	public T replace(long reference, T value) {
		long segment    = (long)(reference / collection.getFragmentSize());
		long offset     = (long)(reference % collection.getFragmentSize());
		Object o = collection.replaceEntity(segment, (int)offset, value);
		return o instanceof Empty? null : (T)o;
	}

	@SuppressWarnings("unchecked")
	public T putIfAbsent(long reference, T value) {
		long segment    = (long)(reference / collection.getFragmentSize());
		long offset     = (long)(reference % collection.getFragmentSize());
		Object o = collection.putIfAbsentEntity(segment, (int)offset, value);
		return o instanceof Empty? null : (T)o;
	}

	public boolean remove(long reference, T oldValue) {
		long segment    = (long)(reference / collection.getFragmentSize());
		long offset     = (long)(reference % collection.getFragmentSize());
		
		if(!EMPTY.equals(collection.replaceEntity(segment, (int)offset, oldValue, EMPTY))){
			this.freeAddress.add(reference);
			return true;
		}
		else{
			return false;
		}
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
