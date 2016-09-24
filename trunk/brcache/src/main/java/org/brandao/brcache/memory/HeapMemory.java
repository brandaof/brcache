package org.brandao.brcache.memory;

/**
 * Gerencia a memória usada no cache.
 * 
 * @author Brandao
 *
 */
public class HeapMemory implements Memory{

	public RegionMemory alloc(long size) {
		byte[][] segs = HeapMemoryUtil.alloc(size);
		return new HeapRegionMemory(segs, (int)HeapMemoryUtil.segmentSize, (int)size);
	}

	public void realloc(long size, RegionMemory region){
		HeapRegionMemory r = (HeapRegionMemory)region;
		byte[][] segs      = r.segments;
		byte[][] newSegs   = HeapMemoryUtil.alloc(size);
		
		if(newSegs.length > segs.length){
			System.arraycopy(segs, 0, newSegs, 0, segs.length);
			HeapMemoryUtil.free(segs);
			r.segments    = newSegs;
			r.segmentSize = (int)HeapMemoryUtil.segmentSize;
		}
		else{
			System.arraycopy(segs, 0, newSegs, 0, newSegs.length);
			HeapMemoryUtil.free(segs);
		}
		
	}
	
	public void alloc(long size, RegionMemory region) {
		HeapRegionMemory r = (HeapRegionMemory)region;
		if(r.segments != null)
			throw new IllegalStateException();
		allocSegments(size, r);
	}
	
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
