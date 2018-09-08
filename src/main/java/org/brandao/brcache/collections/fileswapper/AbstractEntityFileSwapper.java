package org.brandao.brcache.collections.fileswapper;

import java.lang.reflect.Array;

import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.collections.Entry;
import org.brandao.brcache.collections.Swapper;
import org.brandao.entityfilemanager.EntityFile;
import org.brandao.entityfilemanager.EntityFileManager;
import org.brandao.entityfilemanager.tx.EntityFileTransaction;

public abstract class AbstractEntityFileSwapper<T>
	implements Swapper<T>{

	private static final long serialVersionUID = -8560081438455645675L;

	private volatile long maxID;
	
	protected Class<T> type;
	
	@SuppressWarnings("unchecked")
	protected synchronized void allocSpace(EntityFileManager efm, String name, long index, Entry<T> item){
		
		if(maxID > index){
			return;
		}
		
		EntityFileTransaction eft = null;
		try{
			eft = efm.beginTransaction();
			EntityFile<T> ef = efm.getEntityFile(name, eft, type);
			T[] array = (T[]) Array.newInstance(type, (int)(index - maxID));
			long[] ids = ef.insert(array);
			assert ids[ids.length - 1] == index;
			eft.commit();
			maxID = index;
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

	protected void update(EntityFileManager efm, String name, long index, Entry<T> item){
		
		EntityFileTransaction eft = null;
		try{
			eft = efm.beginTransaction();
			EntityFile<T> ef = efm.getEntityFile(name, eft, type);
			ef.update(index, item.getItem());
			eft.commit();
		}
		catch(Throwable e){
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

	protected Entry<T> get(EntityFileManager efm, String name, long index){
		
		EntityFileTransaction eft = null;
		try{
			eft = efm.beginTransaction();
			EntityFile<T> ef = efm.getEntityFile(name, eft, type);
			Entry<T> e = new Entry<T>(index, ef.select(index));
			eft.commit();
			return e;
		}
		catch(Throwable e){
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
	
}
