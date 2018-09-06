package org.brandao.brcache.collections;

public interface FlushableReferenceCollection<T> 
	extends ReferenceCollection<T> {

    void flush();
    
    void setReadOnly(boolean value);
    
    boolean isReadOnly();

    void setDeleteOnExit(boolean value);
    
    boolean isDeleteOnExit();
    
    void destroy();
    
}
