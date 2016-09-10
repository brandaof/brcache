package org.brandao.brcache.collections;

public interface HugeReferenceList<T> 
	extends ReferenceList<T>, HugeCollection<T> {

    void flush();
    
    void setReadOnly(boolean value);
    
    boolean isReadOnly();

    void setDeleteOnExit(boolean value);
    
    boolean isDeleteOnExit();
    
    void destroy();
    
}
