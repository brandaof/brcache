package org.brandao.brcache.collections;

public class FlushableReferenceCollectionImp<T> 
	implements FlushableReferenceCollection<T>{

	private static final long serialVersionUID = 1406571295066759006L;

	private SimpleReferenceCollection<T>[] lists;

    private boolean deleteOnExit;
	
    public FlushableReferenceCollectionImp() {
        this(
            HugeArrayList.DEFAULT_MAX_CAPACITY_ELEMENT, 
            HugeArrayList.DEFAULT_CLEAR_FACTOR_ELEMENT, 
            HugeArrayList.DEFAULT_FRAGMENT_FACTOR_ELEMENT,
            null,
            1,
            1);
    }

    @SuppressWarnings("unchecked")
	public FlushableReferenceCollectionImp(
            int maxCapacityElements,
            double clearFactorElements, 
            double fragmentFactorElements,
            Swapper<T> swap,
            int quantityClearThread, 
            int lists) {
    
    	this.lists        = new SimpleReferenceCollection[lists];
        this.deleteOnExit = true;
    	
    	for(int i=0;i<this.lists.length;i++){
            this.lists[i] = 
                    new SimpleReferenceCollection<T>(
                        maxCapacityElements, 
                        clearFactorElements, 
                        fragmentFactorElements,
                        swap,
                        quantityClearThread);
    		
    	}
    	
    }
    
	public long insert(T e) {
		int threadReference  = (int)(Thread.currentThread().getId() % lists.length);
		long index           = lists[threadReference].insert(e);
		return ((index & 0xffffffffL) << 8) | (threadReference & 0xff);
		/*
		int threadReference  = (int)(Thread.currentThread().getId() % lists.length);
		long index           = lists[threadReference].insert(e);
		
		threadReference = threadReference & 0xff;
		index           = index & 0xffffffffL;
		
		long address = (index << 8) | threadReference;
		return address;
		*/
	}

	public T set(long reference, T e) {
		return lists[(int)(reference & 0xff)].set(reference >> 8, e);
		/*
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].set(index, e);
		*/
	}

	public T get(long reference) {
		return lists[(int)(reference & 0xff)].get(reference >> 8);
		/*
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].get(index);
		*/
	}

	public boolean remove(long reference) {
		return lists[(int)(reference & 0xff)].remove(reference >> 8);
		/*
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].remove(index);
		*/
	}
	
	public boolean replace(long reference, T oldValue, T value) {
		return lists[(int)(reference & 0xff)].replace(reference >> 8, oldValue, value);
		/*
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].replace(index, oldValue, value);
		*/
	}

	public T replace(long reference, T value) {
		return lists[(int)(reference & 0xff)].replace(reference >> 8, value);
		/*
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].replace(index, value);
		*/
	}

	public T putIfAbsent(long reference, T value) {
		return lists[(int)(reference & 0xff)].putIfAbsent(reference >> 8, value);
		/*
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].putIfAbsent(index, value);
		*/
	}

	public boolean remove(long reference, T oldValue) {
		return lists[(int)(reference & 0xff)].remove(reference >> 8, oldValue);
		/*
		long threadReference = reference & 0xff;
		long index           = reference >> 8;
		return this.lists[(int)threadReference].remove(index, oldValue);
		*/
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
    	for(SimpleReferenceCollection<T> l: lists){
    		size += l.length();
    	}
    	return size;
    }

    public boolean isEmpty(){
    	return this.size() == 0;
    }
    
    public void clear() {
    	for(SimpleReferenceCollection<T> l: lists){
    		l.clear();
    	}
    }

    public void destroy(){
    	for(SimpleReferenceCollection<T> l: lists){
    		l.destroy();
    	}
    }
    
    public void flush(){
    	for(SimpleReferenceCollection<T> l: lists){
    		l.flush();
    	}
    	
    }

	public void setReadOnly(boolean value) {
    	for(SimpleReferenceCollection<T> l: lists){
    		l.setReadOnly(value);
    	}
	}

	public boolean isReadOnly() {
    	return lists[0].isReadOnly();
	}

}
