package org.brandao.brcache.memory;

/**
 * Gerencia a memória usada no cache.
 * 
 * @author Brandao
 *
 */
public class UnsafeMemory implements Memory{

	public UnsafeMemory(){
	}
	
	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public RegionMemory alloc(long size){
		long address = UnsafeMemoryUtil.alloc(size);
		return new UnsafeRegionMemory(address, size);
	}

	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public void alloc(long size, RegionMemory region){
		UnsafeRegionMemory r = (UnsafeRegionMemory)region;
		if(r.address != null)
			throw new IllegalStateException();
		
		long address = UnsafeMemoryUtil.alloc(size);
		r.address = address;
	}
	
	/**
	 * Libera uma região da memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public void release(RegionMemory region){
		synchronized(region){
			UnsafeRegionMemory r = (UnsafeRegionMemory)region;
			if(r.address == null)
				return;
			UnsafeMemoryUtil.free(r.address);
			r.address = null;
		}
 	}

}
