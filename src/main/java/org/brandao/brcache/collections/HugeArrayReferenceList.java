package org.brandao.brcache.collections;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class HugeArrayReferenceList<T> implements HugeReferenceList<T>{

	private static final long serialVersionUID = 1406571295066759006L;

	private HugeArrayList<T>[] lists;

	private int segment;
	
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
            int quantityClearThread, int lists) {
    
    	this.segment = 0;
    	this.lists   = new HugeArrayList[lists];
    	
    	maxCapacityElements = maxCapacityElements / lists;
    	
    	for(int i=0;i<this.lists.length;i++){
            this.lists[i] = 
                    new HugeArrayList<T>(
                        id == null? null : id + "list_" + i, 
                        maxCapacityElements, 
                        clearFactorElements, 
                        fragmentFactorElements,
                        swap,
                        quantityClearThread);
    		
    	}
    	
    }
    
	public boolean add(T e) {
		this.insert(e);
		return true;
	}
	
	public long insert(T e) {
		segment    = segment++ % this.lists.length;
		int offset = 0;
		HugeArrayList<T> list = this.lists[segment];
		
		synchronized (list) {
			offset = list.size();
			list.add(e);
		}
		
		int seg  = segment & 0xff;
		long off = offset & 0xffffffffL;
		
		long reference = (off << 8) | seg;
		return reference;
	}

	public T set(long reference, T e) {
		long off = reference & 0xffffffff00L;
		long seg  = reference & 0xffL;

		off = off >> 8;
		
		HugeArrayList<T> list = this.lists[(int)seg];
		
		synchronized (list) {
			T old = list.get((int)off);
			list.set((int)off, e);
			return old;
		}
		
	}

	public T get(long reference) {
		long off = reference & 0xffffffff00L;
		long seg  = reference & 0xffL;

		off = off >> 8;
		
		HugeArrayList<T> list = this.lists[(int)seg];
		
		return list.get((int)off);
	}

	public boolean remove(long reference) {
		long off = reference & 0xffffffff00L;
		long seg  = reference & 0xffL;

		off = off >> 8;
		
		HugeArrayList<T> list = this.lists[(int)seg];
		
		synchronized (list) {
			return list.remove(off);
		}
	}
	
	public boolean replace(long reference, T oldValue, T value) {
		long off = reference & 0xffffffff00L;
		long seg  = reference & 0xffL;

		off = off >> 8;
		
		HugeArrayList<T> list = this.lists[(int)seg];
		synchronized(list){
    		T old = list.get((int)off);
    		if(old != null && old.equals(oldValue)){
	    		list.set((int)off, value);
	    		return true;
    		}
			return false;
		}
	}

	public T replace(long reference, T value) {
		long off = reference & 0xffffffff00L;
		long seg  = reference & 0xffL;

		off = off >> 8;
		
		HugeArrayList<T> list = this.lists[(int)seg];
		synchronized(list){
    		T old = list.get((int)off);
    		if(old != null){
	    		list.set((int)off, value);
    		}
			return old;
		}
	}

	public T putIfAbsentValue(long reference, T value) {
		long off = reference & 0xffffffff00L;
		long seg  = reference & 0xffL;

		off = off >> 8;
		
		HugeArrayList<T> list = this.lists[(int)seg];
		synchronized(list){
    		T old = list.get((int)off);
    		if(old == null){
	    		list.set((int)off, value);
    		}
			return old;
		}
	}

	public boolean remove(long reference, T oldValue) {
		long off = reference & 0xffffffff00L;
		long seg  = reference & 0xffL;

		off = off >> 8;
		
		HugeArrayList<T> list = this.lists[(int)seg];
		synchronized(list){
    		T old = list.get((int)off);
    		if(old != null && old.equals(oldValue)){
	    		list.remove((int)off);
	    		return true;
    		}
			return false;
		}
	}
	
    public void setDeleteOnExit(boolean value){
    	for(HugeArrayList<T> l: this.lists){
    		l.setDeleteOnExit(value);
    	}
    }

    public boolean isDeleteOnExit(){
    	return this.lists[0].isDeleteOnExit();
    }
	
    public int size(){
    	return (int)this.length();
    }
    
    public long length(){
    	long size = 0;
    	for(HugeArrayList<T> l: this.lists){
    		size += l.size();
    	}
    	
    	return size;
    }

    public boolean isEmpty(){
    	return this.size() == 0;
    }
    
    public boolean contains(Object value) {
    	
    	for(HugeArrayList<T> l: this.lists){
    		if(l.contains(value))
    			return true;
    	}
    	
    	return false;
    }

    public void clear() {
    	for(HugeArrayList<T> l: this.lists){
    		l.clear();
    	}
    }

    public void destroy(){
    	for(HugeArrayList<T> l: this.lists){
    		l.destroy();
    	}
    }
    
    public void flush(){
    	for(HugeArrayList<T> l: this.lists){
    		l.flush();
    	}
    	
    }

	public Iterator<T> iterator() {
		return new HugeIterator<T>(this);
	}

	public Object[] toArray() {
        return toArray(new Object[]{});
	}

	@SuppressWarnings("unchecked")
	public <K> K[] toArray(K[] a) {
        K[] result = Arrays.copyOf(a, this.size());
		for(int i=0;i<result.length;i++){
			result[i] = (K)this.get(i);
		}
		return result;
	}

	public boolean remove(Object o) {
        int index = -1;
        
        for(int i=0;i<this.length();i++){
            if(o.equals(get(i))){
                index = i;
                break;
            }
        }
        
        if(index != -1)
            return this.remove(index) != null;
        else
            return false;
	}

	public boolean containsAll(Collection<?> c) {
        for(Object entity: c)
            if(!this.contains(entity))
                return false;
        return true;
	}

	public boolean addAll(Collection<? extends T> c) {
        for(T o: c){
        	this.insert(o);
        }
		return true;
	}

	public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean removeAll(Collection<?> c) {
		boolean result = false;
		
        for(Object o: c){
        	for(HugeArrayList<T> l: this.lists){
        		result |= l.remove(o);
        	}
        }
        
        return result;
	}

	public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not supported yet.");
	}

	public T get(int index) {
		int list = index % this.lists.length;
		int off  = index / this.lists.length;
		return this.lists[list].get(off);
	}

	public T set(int index, T element) {
		int list = index % this.lists.length;
		int off  = index / this.lists.length;
		return this.lists[list].set(off, element);
	}

	public void add(int index, T element) {
		int list = index % this.lists.length;
		int off  = index / this.lists.length;
		this.lists[list].add(off, element);
	}

	public T remove(int index) {
		int list = index % this.lists.length;
		int off  = index / this.lists.length;
		return this.lists[list].remove(off);
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
    	for(HugeArrayList<T> l: this.lists){
    		l.setReadOnly(value);
    	}
	}

	public boolean isReadOnly() {
    	return this.lists[0].isReadOnly();
	}
	
}
