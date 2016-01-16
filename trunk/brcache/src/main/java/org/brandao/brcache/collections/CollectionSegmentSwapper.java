package org.brandao.brcache.collections;

public interface CollectionSegmentSwapper<T> {

    void swapOnDisk(Entry<T> segment);
	
	Entry<T> swapOnMemory(long segment);

}
