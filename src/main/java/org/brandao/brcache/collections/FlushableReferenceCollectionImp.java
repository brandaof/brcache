package org.brandao.brcache.collections;

public class FlushableReferenceCollectionImp<T> 
	implements FlushableReferenceCollection<T>{

	private static final long serialVersionUID = 1406571295066759006L;

	private ReferenceCollectionSegment<T>[] lists;

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
            Swapper swap,
            int quantityClearThread, 
            int lists) {
    
    	this.lists        = new ReferenceCollectionSegment[lists];
        this.deleteOnExit = true;
    	
    	for(int i=0;i<this.lists.length;i++){
            this.lists[i] = 
                    new ReferenceCollectionSegment<T>(
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
    	for(ReferenceCollectionSegment<T> l: this.lists){
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
    	for(ReferenceCollectionSegment<T> l: this.lists){
    		l.clear();
    	}
    }

    public void destroy(){
    	for(ReferenceCollectionSegment<T> l: this.lists){
    		l.destroy();
    	}
    }
    
    public void flush(){
    	for(ReferenceCollectionSegment<T> l: this.lists){
    		l.flush();
    	}
    	
    }

	public void setReadOnly(boolean value) {
    	for(ReferenceCollectionSegment<T> l: this.lists){
    		l.setReadOnly(value);
    	}
	}

	public boolean isReadOnly() {
    	return this.lists[0].isReadOnly();
	}

}
