package org.brandao.brcache.collections;

import java.io.Serializable;
import java.util.Collection;

public interface ReferenceCollection<T> 
	extends Collection<T>, Serializable{

	long insert(T e);

	void set(long reference, T e);
	
	T get(long reference);
	
	boolean remove(long reference);
	
	long length();
	
}
