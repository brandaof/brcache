package org.brandao.brcache.collections;

public interface FlushableReferenceCollection<T> 
	extends ReferenceCollection<T> {

    void flush();
    
}
