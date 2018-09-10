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
	
	public AbstractEntityFileSwapper(){
		this.maxID = -1;
	}
	
	@SuppressWarnings("unchecked")
	protected synchronized void allocSpace(EntityFileManager efm, String name, long index, Entry<T> item){
		
		EntityFileTransaction eft = null;
		try{
			//inicia a transação no arquivo
			eft = efm.beginTransaction();
			//obtém o entityfile da entidade
			EntityFile<T> ef = efm.getEntityFile(name, eft, type);
			//obtém a quantidade de itens ainda não foram inseridos no arquivo
			int emptyInsert = (int)(maxID < 0? index : index - maxID - 1);
			
			if(emptyInsert > 0){
				//reserva espaço para os itens ainda não inseridos no arquivo
				T[] array = (T[]) Array.newInstance(type, emptyInsert);
				long[] ids = ef.insert(array);
				assert ids[ids.length-1] == index - 1;
			}
			
			//insere o item atual
			if(index <= maxID){
				ef.update(index, item.getItem());
			}
			else{
				long newid = ef.insert(item.getItem());
				assert newid == index;
				maxID = newid;
			}
			
			//atualiza a maior id já inserida no arquivo
			//maxID = index;
			
			//confirma a alteração
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

	protected void update(EntityFileManager efm, String name, long index, Entry<T> item){
		
		EntityFileTransaction eft = null;
		try{
			eft = efm.beginTransaction();
			EntityFile<T> ef = efm.getEntityFile(name, eft, type);
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
	
}
