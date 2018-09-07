package org.brandao.brcache.collections.swapper;

import org.brandao.brcache.CacheException;
import org.brandao.brcache.collections.Entry;
import org.brandao.brcache.collections.fileswapper.AbstractEntityFileSwapper;
import org.brandao.entityfilemanager.EntityFileManager;

public class BasicEntityFileSwapper<T> 
	extends AbstractEntityFileSwapper<T>{

	private static final long serialVersionUID = 4746825276737497673L;

	private EntityFileManager efm;

	private String name;
	
	private volatile long maxID;
	
	public BasicEntityFileSwapper(EntityFileManager efm, String name){
		this.efm       = efm;
		this.name      = name;
		this.maxID     = -1;
	}
	
	public void sendItem(long index, Entry<T> item) throws CacheException{
		if(maxID < index){
			super.allocSpace(efm, name, index, item);
		}
		else{
			super.update(efm, name, index, item);
		}
	}
	
	public Entry<T> getItem(long index) throws CacheException{
		return super.get(efm, name, index);
	}

	public synchronized void clear() throws CacheException{
	}

	public synchronized void destroy() throws CacheException{
	}

}
