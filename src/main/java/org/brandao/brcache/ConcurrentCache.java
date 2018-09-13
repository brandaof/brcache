package org.brandao.brcache;

import java.io.InputStream;
import java.io.Serializable;

import org.brandao.concurrent.NamedLock;

/**
 * Provê as operações de um cache com bloqueio dos métodos que alteram o cache.
 * 
 * <pre>
 * ex:
 *    
 *    Cache cache = ...;
 *    cache.put("uma_chave", meuObjeto, 1200, 0);
 *    
 * ex2:
 *    
 *    MeuObjeto o = cache.get("uma_chave");
 *
 * </pre>
 * @author Brandao
 *
 */
public class ConcurrentCache extends BasicCache {
	
	private static final long serialVersionUID = -8558471389768293591L;

	protected transient NamedLock locks;
	
    /**
     * Cria um novo cache a partir de uma configuração específica.
     * @param config configuração.
     */
    public ConcurrentCache(BRCacheConfig config){
    	super(config);
    	this.locks = new NamedLock();
    }
    
    /**
     * Cria um novo cache.
     * 
     * @param cacheHandler Manpulador do cache.
     */
    public ConcurrentCache(CacheHandler cacheHandler){
    	super(cacheHandler);
    	this.locks = new NamedLock();
    }
    
	/* métodos de armazenamento */
	
    /**
     * Substitui o valor associado à chave somente se ele existir.
     * @param key chave associada ao valor.
     * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
	public boolean replace(String key, Object value, 
			long timeToLive, long timeToIdle) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			return super.replace(key, value, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
    /**
     * Substitui o fluxo de bytes associado à chave somente se ele existir.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public boolean replaceStream(String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
		Serializable refLock = this.locks.lock(key);
		try{
			return super.replaceStream(key, inputData, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
    }
	
	/**
	 * Substitui o valor associado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associada ao valor.
	 * @param oldValue valor esperado associado à chave.
	 * @param newValue valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
	 * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
	 */
	public boolean replace(String key, Object oldValue, 
			Object newValue, long timeToLive, long timeToIdle) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = super.get(key);
			if(o != null && o.equals(oldValue)){
				super.put(key, newValue, timeToLive, timeToIdle);
				return true;
			}
			else
				return false;
		}
		catch(StorageException e){
			throw e;
		}
		catch(RecoverException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	/**
	 * Associa o valor à chave somente se a chave não estiver associada a um valor.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
	 * @return valor anterior associado à chave.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
	 */
	public Object putIfAbsent(String key,
			Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			return super.putIfAbsent(key, value, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
    /**
     * Associa o fluxo de bytes do valor à chave somente se a chave não estiver associada a um valor.
     * @param key chave associada ao valor.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return fluxo associado à chave ou <code>null</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public InputStream putIfAbsentStream(String key, InputStream inputData, long timeToLive, long timeToIdle) throws StorageException{
    	
		Serializable refLock = this.locks.lock(key);
		try{
			return super.putIfAbsentStream(key, inputData, timeToLive, timeToIdle);
		}
		catch(StorageException e){
			throw e;
		}
		catch(RecoverException e){
			throw new StorageException(e, e.getError(), e.getParams());
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
    }
	
	/**
	 * Associa o valor à chave.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @return <code>true</code> se o item for substituido. Caso contrário, <code>false</code>
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
	 */
	public boolean put(String key, Object value, long timeToLive, long timeToIdle) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			return super.put(key, value, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
    /**
	 * Associa o fluxo de bytes do valor à chave.
	 * @param key chave associada ao fluxo.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @param inputData fluxo de bytes do valor.
     * @return <code>true</code> se o item for substituido. Caso contrário, <code>false</code>
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o item.
     */
    public boolean putStream(String key, InputStream inputData, 
    		long timeToLive, long timeToIdle) throws StorageException{
		Serializable refLock = this.locks.lock(key);
		try{
			return super.putStream(key, inputData, timeToLive, timeToIdle);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
    }
	
    /* métodos de coleta */
	
	/**
	 * Obtém o valor associado à chave.
	 * @param key chave associada ao valor.
     * @return valor associado à chave ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
	 */
	public Object get(String key) throws RecoverException {
		/* Deixar sem bloqueio */
		
		//Serializable refLock = this.locks.lock(key);
		//try{
			return super.get(key);
		//}
		//finally{
		//	if(refLock != null){
		//		this.locks.unlock(refLock, key);
		//	}
		//}
	}

    /**
     * Obtém o fluxo de bytes do valor associado à chave.
     * @param key chave associada ao fluxo.
     * @return fluxo de bytes do valor ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
     */
    public InputStream getStream(String key) throws RecoverException {
		/* Deixar sem bloqueio */
    	
		//Serializable refLock = this.locks.lock(key);
		//try{
			return super.getStream(key);
		//}
		//finally{
		//	if(refLock != null){
		//		this.locks.unlock(refLock, key);
		//	}
		//}
    }
	
    /* métodos de remoção */

	/**
	 * Remove o valor associado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associada ao valor.
	 * @param value valor esperado associado à chave.
	 * @return <code>true</code> se o valor for removido. Caso contrário, <code>false</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
	 */
	public boolean remove(String key, Object value) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = super.get(key);
			if(o != null && o.equals(value)){
				return super.remove(key);
			}
			else
				return false;
		}
    	catch(StorageException e){
    		throw e;
    	}
    	catch(RecoverException e){
    		throw new StorageException(e, e.getError(), e.getParams());
    	}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
    /**
     * Remove o valor associado à chave.
     * @param key chave associada ao valor.
     * @return <code>true</code> se o valor for removido. Caso contrário <code>false</code>.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
     */
    public boolean remove(String key) throws StorageException{
		Serializable refLock = this.locks.lock(key);
		try{
			return super.remove(key);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
    }
	
	/**
	 * Obtém a quantidade de itens contido no cache.
	 * @return quantidade de itens.
	 */
	public long size() {
		return super.getCountRemoved() - super.getCountWrite();
	}

	/**
	 * Verifica se o cache está vazio.
	 * @return <code>true</code> se o cache estiver vazio. Caso contrário, <code>false</code>.
	 */
	public boolean isEmpty() {
		return this.size() == 0;
	}

	/**
	 * Verifica se uma chave está associado a um valor.
	 * @param key chave associada ao valor.
	 * @return <code>true</code> se existir um valor associado à chave. Caso contrário, <code>false</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
	 */
	public boolean containsKey(String key) throws RecoverException {
		return super.getStream(key) != null;
	}
	
}
