package org.brandao.brcache.memory;

import java.lang.reflect.Field;
import java.util.Random;

import sun.misc.Unsafe;
/**
 * Gerencia a memória usada no cache.
 * 
 * @author Brandao
 *
 */
@SuppressWarnings("restriction")
class Memory {

	static final Unsafe UNSAFE;
	
    static final long BYTE_ARRAY_OFFSET;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
    		BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public static RegionMemory alloc(long size){
		long address = UNSAFE.allocateMemory(size);
		return new RegionMemory(address, size);
	}

	/**
	 * Aloca uma quantidade específica de memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public static void alloc(long size, RegionMemory region){
		if(region.address != null)
			throw new IllegalStateException();
		
		long address = UNSAFE.allocateMemory(size);
		region.address = address;
	}
	
	/**
	 * Libera uma região da memória.
	 * 
	 * @param size Quantidade.
	 * @return Região da memória.
	 */
	public static void release(RegionMemory region){
		if(region.address == null)
			return;
		UNSAFE.freeMemory(region.address);
 	}
	
	static void arrayCopy(byte[] origin, long originOff, byte[] dest, long destOff, long len){
		UNSAFE.copyMemory(origin, BYTE_ARRAY_OFFSET + originOff, dest, BYTE_ARRAY_OFFSET + destOff, len);
	}

	static void arrayCopy(long origin, long originOff, long dest, long destOff, long len){
		UNSAFE.copyMemory(origin + originOff, dest + destOff, len);
	}
	
	static long getAddress(Object obj) {
	    Object[] array = new Object[] {obj};
	    long baseOffset = UNSAFE.arrayBaseOffset(Object[].class);
	    return UNSAFE.getLong(array, baseOffset);
	}
	
	private static long normalize(int value) {
	    if(value >= 0) return value;
	    return (~0L >>> 32) & value;
	}	
	
	public static void main(String[] a){
		RegionMemory buf = Memory.alloc(1024);
		RegionMemory buf2 = Memory.alloc(1024);
		
		byte[] b  = new byte[(int)buf.length()];
		byte[] b2 = new byte[(int)buf.length()];
		Random r = new Random();
		r.nextBytes(b);
		buf.write(b, 0, b.length);
		buf2.write(buf, 0, buf.length);
		buf2.reset();
		buf2.read(b2, 0, b2.length);
	}
	
}
