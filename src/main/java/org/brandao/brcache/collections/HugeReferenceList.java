package org.brandao.brcache.collections;

public class HugeReferenceList<T> implements ReferenceList<T> {

	private static final long serialVersionUID = 455498382990335188L;

	private HugeArrayList<T>[] lists;

	private int segment;
	
    public HugeReferenceList() {
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
	public HugeReferenceList(
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
    
    
	public long add(T e) {
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

	public void set(long reference, T e) {
		long off = reference & 0xffffffff00L;
		long seg  = reference & 0xffL;

		off = off >> 8;
		
		HugeArrayList<T> list = this.lists[(int)seg];
		
		synchronized (list) {
			list.set((int)off, e);
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
}
