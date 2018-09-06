package org.brandao.brcache.collections;

import java.util.concurrent.locks.Lock;

public interface SwapCollection<T> {

    long getId();
    
	void add(Entry<T> item);

    Entry<T> getEntry(long index);
	
    Entry<T> remove(Entry<T> item);

    Entry<T> reload(Entry<T> entity);
    
	boolean isReadOnly();

	void setReadOnly(boolean readOnly);

	int getNumberOfGroups();
	
	Lock getGroupLock(long index);
	
	Lock getLock();

	Swapper getSwap();

	void setForceSwap(boolean value);
	
	boolean isForceSwap();

	long getMaxSegmentCapacity();
    
    void flush();

    void clear();

    void destroy();
    
}
