package org.brandao.brcache.memory;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
class UnsafeMemoryUtil {

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

	public static long alloc(long size){
		return UNSAFE.allocateMemory(size);
	}
    
	public static void free(long address){
		UNSAFE.freeMemory(address);		
	}
	
	public static void arrayCopy(long origin, long originOff, long dest, long destOff, long len){
		UNSAFE.copyMemory(origin + originOff, dest + destOff, len);
	}
	
	public static long getAddress(Object o) {
		Object[] array = new Object[] {o};
		long baseOffset     = UNSAFE.arrayBaseOffset(Object[].class);
		long dataBaseOffset = UNSAFE.arrayBaseOffset(o.getClass());
		int addressSize     = UNSAFE.addressSize();
		
		long address;
		switch (addressSize){
			case 4:
				address = UNSAFE.getInt(array, baseOffset) + dataBaseOffset;
				break;
			case 8:
				address = UNSAFE.getLong(array, baseOffset) + dataBaseOffset;
				break;
			default:
				throw new Error("unsupported address size: " + addressSize);
		}
		return address;
	}
		
	public static Object getObject(long address) {
	    Object[] array = new Object[] {null};
	    long baseOffset = UNSAFE.arrayBaseOffset(Object[].class);
	    UNSAFE.putLong(array, baseOffset, address);
	    return array[0];
	}
	
	private static long normalize(int value) {
	    if(value >= 0) return value;
	    return (~0L >>> 32) & value;
	}	
	
}
