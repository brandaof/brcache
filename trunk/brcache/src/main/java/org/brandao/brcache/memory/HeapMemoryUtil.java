package org.brandao.brcache.memory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class HeapMemoryUtil {

	/**
	 * Blocos de dados.
	 */
	public static int segmentSize = 64;
	
	/**
	 * Blocos alocados e livres.
	 */
	private static BlockingQueue<byte[]> segments = new LinkedBlockingQueue<byte[]>();
	
	public static byte[][] alloc(long size){
		int quantity = (int)(size / HeapMemoryUtil.segmentSize);
		
		if((size % HeapMemoryUtil.segmentSize) > 0)
			quantity++;
		
		byte[][] allocSegs = new byte[quantity][];
		
		for(int i=0;i<quantity;i++){
			byte[] seg = allocSegment();
			allocSegs[i] = seg;
		}
		return allocSegs;
	}
	
	public static void free(byte[][] allocSegs){
		for(int i=0;i<allocSegs.length;i++){
			segments.add(allocSegs[i]);
		}
 	}
	
	private static byte[] allocSegment(){
		byte[] seg = segments.poll();
		if(seg == null)
			return new byte[segmentSize];
		else
			return seg;
	}
	
}
