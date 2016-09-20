package org.brandao.brcache.memory;

/**
 * Gerencia a memória usada no cache.
 * 
 * @author Brandao
 *
 */
public class HeapMemory implements Memory{

	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public RegionMemory alloc(long size) {
		byte[][] segs = HeapMemoryUtil.alloc(size);
		return new HeapRegionMemory(segs, (int)HeapMemoryUtil.segmentSize, (int)size);
	}

	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public void alloc(long size, RegionMemory region) {
		HeapRegionMemory r = (HeapRegionMemory)region;
		if(r.segments != null)
			throw new IllegalStateException();
		allocSegments(size, r);
	}
	
	/**
	 * Libera uma região da memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public void release(RegionMemory region){
		HeapRegionMemory r = (HeapRegionMemory)region;
		if(r.segments == null)
			return;
		
		HeapMemoryUtil.free(r.segments);
		r.segments = null;
 	}
	
	private HeapRegionMemory allocSegments(long size, HeapRegionMemory region){
		byte[][] allocSegs = HeapMemoryUtil.alloc(size);
		region.length      = size;
		region.segments    = allocSegs;
		region.segmentSize = HeapMemoryUtil.segmentSize;
		return region;
	}
	

}
