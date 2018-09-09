package org.brandao.brcache.collections;

import java.util.concurrent.locks.Lock;

public interface SwapCollection<T> {

    long getId();

	long add(T item);
    
	T set(long index, T item);

	T get(long index);

	boolean replace(long index, T oldValue, T item);

	T replace(long index, T item);

	T putIfAbsent(long index, T item);
	
	boolean isReadOnly();

	void setReadOnly(boolean readOnly);

	int getNumberOfGroups();
	
	Lock getGroupLock(long index);
	
	Lock getLock();

	Swapper<T> getSwap();

	void setForceSwap(boolean value);
	
	boolean isForceSwap();

	long getMaxSegmentCapacity();
    
    void flush();

    void clear();

    void destroy();
    
}
