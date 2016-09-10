package org.brandao.brcache.collections;

import java.io.Serializable;

public interface ReferenceList<T> extends Serializable{

	long add(T e);

	void set(long reference, T e);
	
	T get(long reference);
	
	boolean remove(long reference);
	
}
