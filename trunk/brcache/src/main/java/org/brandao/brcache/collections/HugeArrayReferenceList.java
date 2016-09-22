package org.brandao.brcache.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.brandao.brcache.collections.swapper.TreeFileSwaper;

public class HugeArrayReferenceList<T> 
	implements HugeReferenceList<T>{

	private static final long serialVersionUID = 1406571295066759006L;

	private ArrayCollectionReference<T>[] lists;

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
    
    	this.lists        = new ArrayCollectionReference[lists];
        this.deleteOnExit = true;
        id                = id == null? Collections.getNextId() : id;
        swap              = swap == null? new TreeFileSwaper() : swap;
    	
    	for(int i=0;i<this.lists.length;i++){
            this.lists[i] = 
                    new ArrayCollectionReference<T>(
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
		long threadReference = Thread.currentThread().getId() % this.lists.length;
		long index           = this.lists[(int)threadReference].insert(e);
		
		threadReference = threadReference & 0xff;
		index           = index & 0xffffffffL;
		
		long address = (index << 8) | threadReference;
		return address;
	}

	public T set(long reference, T e) {
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].set(index, e);
	}

	public T get(long reference) {
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].get(index);
	}

	public boolean remove(long reference) {
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].remove(index);
	}
	
	public boolean replace(long reference, T oldValue, T value) {
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].replace(index, oldValue, value);
	}

	public T replace(long reference, T value) {
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].replace(index, value);
	}

	public T putIfAbsent(long reference, T value) {
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].putIfAbsent(index, value);
	}

	public boolean remove(long reference, T oldValue) {
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].remove(index, oldValue);
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
    	long size = 0;
    	for(ArrayCollectionReference<T> l: this.lists){
    		size += l.length();
    	}
    	return size;
    }

    public boolean isEmpty(){
    	return this.size() == 0;
    }
    
    public boolean contains(Object value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void clear() {
    	for(ArrayCollectionReference<T> l: this.lists){
    		l.clear();
    	}
    }

    public void destroy(){
    	for(ArrayCollectionReference<T> l: this.lists){
    		l.destroy();
    	}
    }
    
    public void flush(){
    	for(ArrayCollectionReference<T> l: this.lists){
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
    	for(ArrayCollectionReference<T> l: this.lists){
    		l.setReadOnly(value);
    	}
	}

	public boolean isReadOnly() {
    	return this.lists[0].isReadOnly();
	}

}
