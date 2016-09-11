package org.brandao.brcache.collections;

import java.io.Serializable;
import java.util.Collection;

public interface ReferenceCollection<T> 
	extends Collection<T>, Serializable{

	long insert(T e);

	T set(long reference, T e);
	
	T get(long reference);
	
	boolean remove(long reference);
	
	long length();
	
    boolean replace(long reference, T oldValue, T value);

    T replace(long reference, T value);
    
    T putIfAbsent(long reference, T value);
    
    boolean remove(long reference, T oldValue);
	
}
