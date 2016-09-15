package org.brandao.brcache.collections;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class HugeArrayReferenceList<T> 
	implements HugeReferenceList<T>{

	private static final long serialVersionUID = 1406571295066759006L;

	private static final Empty EMPTY = new Empty();
	
	private BlockingQueue<Long> freeAddress;
	
	private CollectionSegmentImp<Object>[] lists;

	private volatile long lastPos;
	
    private boolean deleteOnExit;
	
    public HugeArrayReferenceList() {
        this(
            null, 
            HugeArrayList.DEFAULT_MAX_CAPACITY_ELEMENT, 
            HugeArrayList.DEFAULT_CLEAR_FACTOR_ELEMENT, 
            HugeArrayList.DEFAULT_FRAGMENT_FACTOR_ELEMENT,
            null,
            1,
            1);
    }

    @SuppressWarnings("unchecked")
	public HugeArrayReferenceList(
            String id, 
            int maxCapacityElements,
            double clearFactorElements, 
            double fragmentFactorElements,
            Swapper swap,
            int quantityClearThread, 
            int lists) {
    
    	this.freeAddress  = new LinkedBlockingQueue<Long>();
    	this.lastPos      = 0;
    	this.lists        = new CollectionSegmentImp[lists];
        this.deleteOnExit = true;
        id                = id == null? Collections.getNextId() : id;
        swap              = swap == null? new TreeFileSwaper() : swap;
    	
    	for(int i=0;i<this.lists.length;i++){
            this.lists[i] = 
                    new CollectionSegmentImp<Object>(
                        id == null? null : id + "list_" + i, 
                        maxCapacityElements, 
                        clearFactorElements, 
                        fragmentFactorElements,
                        swap.clone(),
                        quantityClearThread);
    		
    	}
    	
    }
    
	public boolean add(T e) {
		this.insert(e);
		return true;
	}
	
	public long insert(T e) {
		
		Long address = this.freeAddress.poll();
		
		if(address == null){
			long currentPos      = this.lastPos++;
			long collectionIndex = currentPos % this.lists.length;
			long index           = currentPos / this.lists.length;
			
			CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
			long segment    = (long)(index / collection.getFragmentSize());
			long offset     = (long)(index % collection.getFragmentSize());
	
			int seg         = (int)(collectionIndex & 0xff);
			long off        = index & 0xffffffffL;
			
			address = (off << 8) | seg;
			
			try{
				collection.putEntity(segment, (int)offset, e);
			}
			catch(Throwable ex){
				this.freeAddress.add(address);
			}
			
		}
		else{
			long collectionIndex = address & 0xffL;
			long index           = address & 0xffffffff00L;
			index                = index >> 8;

			CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
			
			long segment    = (long)(index / collection.getFragmentSize());
			long offset     = (long)(index % collection.getFragmentSize());
			
			try{
				collection.putEntity(segment, (int)offset, e);
			}
			catch(Throwable ex){
				this.freeAddress.add(address);
			}
			
		}
		
		System.out.println(address);
		return address;
	}

	@SuppressWarnings("unchecked")
	public T set(long reference, T e) {
		
		long collectionIndex = reference & 0xffL;
		long index           = reference & 0xffffffff00L;
		index                = index >> 8;
			
		CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
		
		long segment    = (long)(index / collection.getFragmentSize());
		long offset     = (long)(index % collection.getFragmentSize());
		
		return (T)collection.setEntity(segment, (int)offset, (T)e);
	}

	@SuppressWarnings("unchecked")
	public T get(long reference) {
		long collectionIndex = reference & 0xffL;
		long index           = reference & 0xffffffff00L;
		index                = index >> 8;

		CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
		
		long segment    = (long)(index / collection.getFragmentSize());
		long offset     = (long)(index % collection.getFragmentSize());
		
		return (T)collection.getEntity(segment, (int)offset);
	}

	public boolean remove(long reference) {
		
		long collectionIndex = reference & 0xffL;
		long index           = reference & 0xffffffff00L;
		index                = index >> 8;

		CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
		
		long segment    = (long)(index / collection.getFragmentSize());
		long offset     = (long)(index % collection.getFragmentSize());
		
		if(!EMPTY.equals(collection.replaceEntity(segment, (int)offset, EMPTY))){
			this.freeAddress.add(reference);
			return true;
		}
		else{
			return false;
		}
	}
	
	public boolean replace(long reference, T oldValue, T value) {
		
		long collectionIndex = reference & 0xffL;
		long index           = reference & 0xffffffff00L;
		index                = index >> 8;

		CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
		
		long segment    = (long)(index / collection.getFragmentSize());
		long offset     = (long)(index % collection.getFragmentSize());
		
		return collection.replaceEntity(segment, (int)offset, oldValue, value);
	}

	@SuppressWarnings("unchecked")
	public T replace(long reference, T value) {
		
		long collectionIndex = reference & 0xffL;
		long index           = reference & 0xffffffff00L;
		index                = index >> 8;

		CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
		
		long segment    = (long)(index / collection.getFragmentSize());
		long offset     = (long)(index % collection.getFragmentSize());
		
		return (T)collection.replaceEntity(segment, (int)offset, value);
	}

	@SuppressWarnings("unchecked")
	public T putIfAbsent(long reference, T value) {
		
		long collectionIndex = reference & 0xffL;
		long index           = reference & 0xffffffff00L;
		index                = index >> 8;

		CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
		
		long segment    = (long)(index / collection.getFragmentSize());
		long offset     = (long)(index % collection.getFragmentSize());
		
		return (T)collection.putIfAbsentEntity(segment, (int)offset, value);
	}

	public boolean remove(long reference, T oldValue) {
		
		long collectionIndex = reference & 0xffL;
		long index           = reference & 0xffffffff00L;
		index                = index >> 8;

		CollectionSegmentImp<Object> collection = this.lists[(int)collectionIndex];
		
		long segment    = (long)(index / collection.getFragmentSize());
		long offset     = (long)(index % collection.getFragmentSize());
		
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
    	for(CollectionSegmentImp<Object> l: this.lists){
    		l.clear();
    	}
    }

    public void destroy(){
    	for(CollectionSegmentImp<Object> l: this.lists){
    		l.destroy();
    	}
    }
    
    public void flush(){
    	for(CollectionSegmentImp<Object> l: this.lists){
    		l.flush();
    	}
    	
    }

	public Iterator<T> iterator() {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public Object[] toArray() {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public <K> K[] toArray(K[] a) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean remove(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public T get(int index) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public T set(int index, T element) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public void add(int index, T element) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public T remove(int index) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public int indexOf(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public void setReadOnly(boolean value) {
    	for(CollectionSegmentImp<Object> l: this.lists){
    		l.setReadOnly(value);
    	}
	}

	public boolean isReadOnly() {
    	return this.lists[0].isReadOnly();
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
