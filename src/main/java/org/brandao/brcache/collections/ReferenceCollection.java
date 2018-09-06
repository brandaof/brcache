package org.brandao.brcache.collections;

import java.io.Serializable;

public interface ReferenceCollection<T> 
	extends Serializable{

	boolean isEmpty();
	
	long insert(T e);

	T set(long reference, T e);
	
	T get(long reference);
	
	boolean remove(long reference);
	
	long length();
	
    boolean replace(long reference, T oldValue, T value);

    T replace(long reference, T value);
    
    T putIfAbsent(long reference, T value);
    
    boolean remove(long reference, T oldValue);
	
    void destroy();

    void clear();
    
    void setReadOnly(boolean value);
    
    boolean isReadOnly();

    void setDeleteOnExit(boolean value);
    
    boolean isDeleteOnExit();
    
}
