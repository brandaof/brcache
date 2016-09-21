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
		r.write(0, b, 0, b.length);
		
		byte[] b2 = new byte[1024];
		r.read(0, b2, 0, b2.length);
		assertTrue(Arrays.equals(b, b2));
	}
	
	public void testCopyAlloc(){
		RegionMemory r = memory.alloc(1024);
		byte[] b = new byte[1024];
		Random random = new Random();
		random.nextBytes(b);
		r.write(0, b, 0, b.length);
		
		byte[] b2 = new byte[1024];
		r.read(0, b2, 0, b2.length);
		assertTrue(Arrays.equals(b, b2));
	}

	public void testCopy2(){
		RegionMemory r = memory.alloc(1024);
		byte[] b = new byte[1024];
		Random random = new Random();
		random.nextBytes(b);
		r.write(10, b, 10, b.length);
		
		byte[] b2 = new byte[1024];
		r.read(10, b2, 10, b2.length);
		
		for(int i= 0;i<10;i++){
			b[i] = 0;
		}
		
		assertTrue(Arrays.equals(b, b2));
	}
	
}
