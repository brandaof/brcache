package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.brandao.brcache.tx.CacheTransactionManager;
import org.brandao.brcache.tx.TXCache;
import org.brandao.concurrent.NamedLock;

/**
 * É a classe central do BRCache. 
 * <p>Ele faz o mapeamento chave-valor. Uma chave 
 * somente pode estar associado a um valor. Não são
 * permtidos chaves duplicadas.</p> 
 * 
 * <pre>
 * ex:
 *    
 *    Cache cache = ...;
 *    cache.put("uma_chave", meuObjeto, 1200);
 *    
 * ex2:
 *    
 *    MeuObjeto o = cache.get("uma_chave");
 *
 * </pre>
 * @author Brandao
 *
 */
public class Cache 
	extends StreamCache {
	
	private static final long serialVersionUID = -8558471389768293591L;

	protected transient NamedLock locks;
	
	protected BRCacheConfig config;
	
    /**
     * Cria um novo cache.
     * 
     */
    public Cache(){
    	this(new BRCacheConfig(new Configuration()));
    	/*
        this(
    		3L*1024L*1024L, 1024, 0.5, 
    		1L*1024L*1024L, 1024, 0.5, 
    		10L*1024L*1024L, 64*1024, 1*1024, 0.5, 
    		1*1024*1024L, 100, "/mnt/brcache", SwaperStrategy.FILE, 1);
		*/
    }
    
    public Cache(BRCacheConfig config){
    	this(
    			config.getNodesBufferSize(), config.getNodesPageSize(), config.getNodesSwapFactor(),
    			config.getIndexBufferSize(), config.getIndexPageSize(), config.getIndexSwapFactor(), 
    			config.getDataBufferSize(), config.getDataPageSize(), config.getDataBlockSize(), config.getDataSwapFactor(), 
    			config.getMaxSizeEntry(), config.getMaxSizeKey(), config.getDataPath(), config.getSwapper(), config.getSwapperThread());
    	this.config = config;
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
    private Cache(
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
     * @param timeout tempo limite. É o tempo máximo que se espera, em milisegundos, para concluir uma operação
     * no cache.
     * @return cache com suporte transacional.
     */
    public TXCache getTXCache(CacheTransactionManager txManager, long timeout){
    	return new TXCache(this, txManager, timeout);
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
			Object o = this.get(key);
			if(o != null){
				this.put(key, value, timeToLive, timeToIdle);
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
			Object o = this.get(key);
			if(o != null && o.equals(oldValue)){
				this.put(key, newValue, timeToLive, timeToIdle);
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
			Object o = this.get(key);
			if(o == null){
				this.put(key, value, timeToLive, timeToIdle);
			}
			
			return o;
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
		try{
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			ObjectOutputStream oout = new ObjectOutputStream(bout);
			oout.writeObject(value);
			oout.flush();
			return this.putStream(key, timeToLive, timeToIdle, new ByteArrayInputStream(bout.toByteArray()));
		}
		catch(StorageException e){
			throw e;
		}
		catch(Throwable e){
			throw new StorageException(e, CacheErrors.ERROR_1020);
		}
	}

	/* métodos de coleta*/
	
	/**
	 * Obtém o valor associado à chave.
	 * @param key chave associada ao valor.
     * @return valor associado à chave ou <code>null</code>.
     * @throws RecoverException Lançada se ocorrer alguma falha ao tentar obter o
     * item.
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
			throw new StorageException(e, CacheErrors.ERROR_1021);
		}
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
			Object o = this.get(key);
			if(o != null && o.equals(value)){
				return this.remove(key);
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
	 * @return <code>true</code> se o valor for removido. Caso contrário, <code>false</code>.
	 * @throws StorageException Lançada se ocorrer alguma falha ao tentar remover o
     * item.
	 */
	public boolean remove(String key) throws StorageException {
		return super.remove(key);
	}

    /* métodos de manipulação*/
    
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

	/**
	 * Obtém a configuração do cache.
	 * @return configuração.
	 */
	public BRCacheConfig getConfig() {
		return config;
	}
	
}
