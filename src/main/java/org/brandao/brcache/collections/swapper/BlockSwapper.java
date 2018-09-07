package org.brandao.brcache.collections.swapper;

import org.brandao.brcache.CacheErrors;
import org.brandao.brcache.CacheException;
import org.brandao.brcache.collections.Entry;
import org.brandao.brcache.collections.Swapper;
import org.brandao.brcache.collections.fileswapper.DataBlock;
import org.brandao.entityfilemanager.EntityFileManager;
import org.brandao.entityfilemanager.tx.EntityFileTransaction;

public class BlockSwapper 
	implements Swapper {

	private static final long serialVersionUID = 4746825276737497673L;

	private EntityFileManager efm;

	private String name;
	
	public BlockSwapper(EntityFileManager efm, String name){
		this.efm  = efm;
		this.name = name;
	}
	
	public void sendItem(long index, Entry<?> item) throws CacheException{
		EntityFileTransaction eft = null;
		try{
			Entry<DataBlock> e = (Entry<DataBlock>)item;
			eft = efm.beginTransaction();
			if()
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

	public Entry<?> getItem(long index) throws CacheException{
		return null;
	}

	public void clear() throws CacheException{
		// TODO Auto-generated method stub
		
	}

	public void destroy() throws CacheException{
		// TODO Auto-generated method stub
		
	}

	public static class DiskEntry<?>
}
