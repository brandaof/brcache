package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.concurrent.NamedLock;

/**
 * Faz o mapeamento chave valor. Uma chave somente pode 
 * estar associado a um valor. Não são permtidos chaves
 * duplicadas. 
 * 
 * <pre>
 * ex:
 *    
 *    Cache cache = ...;
 *    cache.put("umaChave", meuObjeto, 1200);
 *    
 * ex2:
 *    
 *    MeuObjeto o = cache.get("umaChave");
 *
 * </pre>
 * @author Brandao
 *
 */
public class Cache 
	extends StreamCache {
	
	private static final long serialVersionUID = -8558471389768293591L;

	protected transient NamedLock locks;
	
    /**
     * Cria um novo cache.
     * 
     */
    public Cache(){
        super(
    		3L*1024L*1024L, 1024, 0.5, 
    		1L*1024L*1024L, 1024, 0.5, 
    		10L*1024L*1024L, 64*1024, 1*1024, 0.5, 
    		1*1024*1024L, 100, "/mnt/brcache", SwaperStrategy.FILE, 1);
    }
    
    /**
     * Cria um novo cache.
     * 
     * @param nodeBufferSize Tamanho do buffer, em bytes, onde os nós ficarão armazenados. 
     * @param nodePageSize Tamanho da página, em bytes, do buffer de nós.
     * @param nodeSwapFactor Fator de permuta dos nós.
     * @param indexBufferSize Tamanho do buffer, em bytes, onde os índices ficarão armazenados.
     * @param indexPageSize Tamanho da página, em bytes, do buffer de índices.
     * @param indexSwapFactor Fator de permuta dos índices.
     * @param dataBufferSize Tamanho do buffer, em bytes, onde os dados ficarão armazenados. 
     * @param dataPageSize Tamanho da página, em bytes, do buffer de dados.
     * @param blockSize Tamanho do bloco, em bytes.
     * @param dataSwapFactor Fator de permuta dos dados.
     * @param maxSizeEntry Tamanho máximo de uma entrada no cache.
     * @param maxSizeKey Tamanho máximo de uma chave.
     * @param dataPath Pasta onde os dados do cache serão persistidos.
     * @param swaperType Estratégia de armazenamento dos dados em disco.
     * @param quantitySwaperThread Quantidade de processos usados para fazer a permuta.
     */
    public Cache(
    		long nodeBufferSize,
    		long nodePageSize,
    		double nodeSwapFactor,
    		
    		long indexBufferSize,
    		long indexPageSize,
    		double indexSwapFactor,
    		
    		long dataBufferSize,
    		long dataPageSize,
    		long blockSize,
    		double dataSwapFactor,
    		
    		long maxSizeEntry,
    		int maxSizeKey,
            String dataPath,
            SwaperStrategy swaperType,
            int quantitySwaperThread
    		){	
    	super(nodeBufferSize, nodePageSize, nodeSwapFactor, indexBufferSize, 
    			indexPageSize, indexSwapFactor, dataBufferSize, dataPageSize, blockSize, 
    			dataSwapFactor, maxSizeEntry, maxSizeKey, dataPath, swaperType, quantitySwaperThread);
    	this.locks = new NamedLock();
    }
    
    /**
     * Obtém o cache com suporte transacional.
     * @return cache.
     */
    public TXCache getTXCache(){
    	return new TXCache(this);
    }

    /**
     * Obtém o cache com suporte transacional com um gestor transacional e tempo limite específicos.
     * @param txManager gestor transacional.
     * @param timeout tempo limite. É o tempo máximo que se espera em ms para concluir uma operação
     * no cache.
     * @return cache
     */
    public TXCache getTXCache(CacheTransactionManager txManager, long timeout){
    	return new TXCache(this, txManager, timeout);
    }
    
	/* métodos de armazenamento */
	
    /**
     * Substitui o valor associado à chave somente se ele existir.
     * @param key chave associado ao valor.
     * @param value valor para ser associado à chave.
     * @param maxAliveTime tempo máximo de vida do valor no cache.
     * @return o valor anterior associado à chave.
     * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
     */
	public Object replace(String key, Object value, 
			long maxAliveTime) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = this.get(key);
			if(o != null){
				this.put(key, value, maxAliveTime);
				return true;
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	/**
	 * Substitui o valor associado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associado ao valor.
	 * @param oldValue valor esperado associado à chave.
	 * @param newValue valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
	 * @return <code>verdadeiro</code> se o valor for substituido. Caso contrário, <code>falso</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
	 */
	public boolean replace(String key, Object oldValue, 
			Object newValue, long maxAliveTime) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = this.get(key);
			if(o != null && o.equals(oldValue)){
				this.put(key, newValue, maxAliveTime);
				return true;
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	/**
	 * Associa o valor a chave somente se a chave não estiver associada a um valor.
	 * @param key chave associado ao valor.
	 * @param value valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
	 * @return valor anterior associado à chave.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
	 */
	public Object putIfAbsent(String key, 
			Object value, long maxAliveTime) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = this.get(key);
			if(o == null){
				this.put(key, value, maxAliveTime);
			}
			
			return o;
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	/**
	 * Associa uma valor a uma chave.
	 * @param key chave associado ao valor.
	 * @param value valor para ser associado à chave.
	 * @param maxAliveTime tempo máximo de vida do valor no cache.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar inserir o
     * item no cache.
	 */
	public void put(String key, Object value, long maxAliveTime) throws StorageException {
		try{
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
			this.putStream(key, maxAliveTime, new ByteArrayInputStream(bout.toByteArray()));
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
	}

	/* métodos de coleta*/
	
	/**
	 * Obtém o valor associado à chave.
	 * @param key chave associado ao valor.
	 * @return valor para ser associado à chave.
	 * @throws RecoverException Lançada se ocorrer alguma falha ao tentar recuperar o
     * item do cache.
	 */
	public Object get(String key) throws RecoverException {
		try{
			InputStream in = super.getStream(key);
			if(in != null){
					ObjectInputStream oin = new ObjectInputStream(in);
					return oin.readObject();
			}
			else
				return null;
		}
		catch(RecoverException e){
			throw e;
		}	
		catch(Throwable e){
			throw new RecoverException(e);
		}	
	}

    /* métodos de remoção */

	/**
	 * Remove o valor assoiado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associado ao valor.
	 * @return valor para ser associado à chave.
	 * @return <code>verdadeiro</code> se o valor for removido. Caso contrário, <code>falso</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
	 */
	public boolean remove(String key, Object value) throws StorageException {
		
		Serializable refLock = this.locks.lock(key);
		try{
			Object o = this.get(key);
			if(o != null && o.equals(value)){
				return this.remove(key);
			}
			else
				return false;
		}
		catch(Throwable e){
			throw new StorageException(e);
		}
		finally{
			if(refLock != null){
				this.locks.unlock(refLock, key);
			}
		}
	}
	
	/**
	 * Remove o valor associado à chave.
	 * @param key chave associado ao valor.
	 * @return <code>verdadeiro</code> se o valor for removido. Caso contrário, <code>falso</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item do cache.
	 */
	public boolean remove(String key) throws StorageException {
		return super.remove(key);
	}

    /* métodos de manipulação*/
    
	public long size() {
		return super.getCountRemoved() - super.getCountWrite();
	}

	public boolean isEmpty() {
		return super.getCountWrite() == 0;
	}

	public boolean containsKey(String key) throws RecoverException {
		return super.getStream(key) != null;
	}
	
}
