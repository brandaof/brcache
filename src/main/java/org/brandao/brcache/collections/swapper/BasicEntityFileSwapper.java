package org.brandao.brcache.collections.swapper;

import java.lang.reflect.Array;

import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.collections.Entry;
import org.brandao.brcache.collections.Swapper;
import org.brandao.entityfilemanager.EntityFile;
import org.brandao.entityfilemanager.EntityFileManager;
import org.brandao.entityfilemanager.tx.EntityFileTransaction;

public class BasicEntityFileSwapper<T> implements Swapper<T>{

	private static final long serialVersionUID = -6380145781552093583L;

	private EntityFileManager efm;

	private String name;
	
	private volatile long maxID;
	
	protected Class<T> type;
	
	@SuppressWarnings("unchecked")
	public BasicEntityFileSwapper(EntityFileManager efm, String name, Class<?> type){
		this.efm   = efm;
		this.name  = name;
		this.type  = (Class<T>) type;
	}
	
	@SuppressWarnings("unchecked")
	public void sendItem(long index, Entry<T> item) throws CacheException{
		assert index == item.getIndex();
		
		EntityFileTransaction eft = null;
		try{
			eft = efm.beginTransaction();
				
			EntityFile<T> ef = efm.getEntityFile(name, eft, type);
			
			if(maxID <= index){
				synchronized(this){
					if(maxID <= index){
						int emptyInsert = (int)(maxID < 0? index : index - maxID) + 1;
						T[] array = (T[]) Array.newInstance(type, emptyInsert);
						long[] ids = ef.insert(array);
						maxID = ids[ids.length-1];
					}
				}
			}
			
			ef.update(index, item.getItem());
			
			eft.commit();
		}
		catch(Throwable e){
			e.printStackTrace();
			if(eft != null){
				try{
					eft.rollback();
				}
				catch(Throwable x){
					throw new CacheException(
							CacheErrors.ERROR_1014, 
							new IllegalStateException(x.toString(), e));
				}
			}
			throw new CacheException(CacheErrors.ERROR_1014, e);
		}		
	}
	
	public Entry<T> getItem(long index) throws CacheException{

		
		EntityFileTransaction eft = null;
		try{
			eft = efm.beginTransaction();
			EntityFile<T> ef = efm.getEntityFile(name, eft, type);
			Entry<T> e = new Entry<T>(index, ef.select(index, true));
			eft.rollback();
			assert index == e.getIndex();
			e.setNeedUpdate(false);
			return e;
		}
		catch(Throwable e){
			e.printStackTrace();
			if(eft != null){
				try{
					eft.rollback();
				}
				catch(Throwable x){
					throw new CacheException(
							CacheErrors.ERROR_1015, 
							new IllegalStateException(x.toString(), e));
				}
			}
			throw new CacheException(CacheErrors.ERROR_1015, e);
		}
		
	}

	public synchronized void clear() throws CacheException{
	}

	public synchronized void destroy() throws CacheException{
	}

}
