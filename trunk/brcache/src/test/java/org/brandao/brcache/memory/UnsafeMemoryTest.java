package org.brandao.brcache.memory;

import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

public class UnsafeMemoryTest extends TestCase{

	private Memory memory = new UnsafeMemory();
	
	public void testAlloc(){
		RegionMemory r = memory.alloc(1024);
		byte[] b = new byte[1024];
		Random random = new Random();
		random.nextBytes(b);
		r.write(b, 0, b.length);
		
		r.reset();
		byte[] b2 = new byte[1024];
		r.read(b2, 0, b2.length);
		assertTrue(Arrays.equals(b, b2));
	}
}
