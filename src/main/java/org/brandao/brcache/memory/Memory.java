package org.brandao.brcache.memory;

/**
 * Gerencia a memória usada no cache.
 * 
 * @author Brandao
 *
 */
public interface Memory {
	
	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	RegionMemory alloc(long size);

	void realloc(long size, RegionMemory region);
	
	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	void alloc(long size, RegionMemory region);
	
	/**
	 * Libera uma região da memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	void release(RegionMemory region);
	
}
