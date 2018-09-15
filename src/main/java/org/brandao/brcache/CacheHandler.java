package org.brandao.brcache;

import java.io.InputStream;
import java.io.Serializable;

public interface CacheHandler extends Serializable{
    
    boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException;

    boolean replaceStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException;
    
    InputStream putIfAbsentStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException;
    
    InputStream getStream(String key) throws RecoverException;
    
    boolean removeStream(String key) throws StorageException;
    
    boolean containsKey(String key);

    DataMap getPointer(String key) throws RecoverException;

    void setPointer(String key, DataMap newDta) throws RecoverException;
    
    boolean replacePointer(String key, DataMap originalDta, DataMap newDta) throws RecoverException;
    
    void remove(String key, DataMap data);
    
    void releaseSegments(DataMap map);
    
    InputStream getStream(String key, DataMap map) throws RecoverException;
    
    void putData(DataMap map, InputStream inputData) throws StorageException, InterruptedException;

    long getNextModCount();
    
    int getMaxKeySize();
    
	BRCacheConfig getConfig();
    
    long getCountRead();

    long getCountWrite();

    long getCountRemoved();

    long getCountReadData();
    
    long getCountWriteData();

    long getCountRemovedData();
    
    boolean isDeleteOnExit();

	void setDeleteOnExit(boolean deleteOnExit);

	long size();
	
	boolean isEmpty();
	
	void clear();
	
	void destroy();
	
}
