package org.brandao.brcache.collections;

public interface MapReferenceCollection<K,T> {

    T put(K key, T element);

    boolean replace(K key, T oldElement, T element);

    T replace(K key, T element);

    T putIfAbsent(K key, T element);
    
    T get(Object key);

    T remove(Object key);

	boolean remove(Object key, Object oldValue);

    void clear();

    void destroy();
    
    void flush();
	
    void setDeleteOnExit(boolean value);

    boolean isDeleteOnExit();
    
    void setReadOnly(boolean value);
    
    boolean isReadOnly();
    
}
