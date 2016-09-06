package org.brandao.brcache;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Gerencia a memória usada no cache.
 * 
 * @author Brandao
 *
 */
class Memory {

	/**
	 * Blocos de dados.
	 */
	private static int segmentSize = 64;
	
	/**
	 * Blocos alocados e livres.
	 */
	private static BlockingQueue<byte[]> segments = new LinkedBlockingQueue<byte[]>();

	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public static RegionMemory alloc(int size){
		int quantity = (int)(size / segmentSize);
		
		if((size % segmentSize) > 0)
			quantity++;
		
		byte[][] allocSegs = new byte[quantity][];
		
		for(int i=0;i<quantity;i++){
			byte[] seg = allocSegment();
			allocSegs[i] = seg;
		}
		
		return new RegionMemory(allocSegs, (int)segmentSize, size);
	}

	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public static void alloc(int size, RegionMemory region){
		if(region.segments != null)
			throw new IllegalStateException();
		allocSegments(size, region);
	}
	
	/**
	 * Libera uma região da memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public static void release(RegionMemory region){
		if(region.segments == null)
			return;
		
		byte[][] allocSegs = region.getSegments();
		for(int i=0;i<allocSegs.length;i++){
			segments.add(allocSegs[i]);
		}
		
		region.segments = null;
 	}
	
	private static RegionMemory allocSegments(int size, RegionMemory region){
		int quantity = (int)(size / segmentSize);
		
		if((size % segmentSize) > 0)
			quantity++;
		
		byte[][] allocSegs = new byte[quantity][];
		
		for(int i=0;i<quantity;i++){
			byte[] seg = allocSegment();
			allocSegs[i] = seg;
		}
		
		region.length      = size;
		region.segments    = allocSegs;
		region.segmentSize = segmentSize;
		return region;
	}
	
	private static byte[] allocSegment(){
		byte[] seg = segments.poll();
		if(seg == null)
			return new byte[segmentSize];
		else
			return seg;
	}
}
