package org.brandao.brcache.collections;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RouletteLock {

	private Lock[] locks;

	public RouletteLock(){
		this(50);
	}
	
	public RouletteLock(int size){
		this.locks = new Lock[size];
		for(int i=0;i<size;i++){
			this.locks[i] = new ReentrantLock();
		}
	}
	
	public Lock getLock(long value){
		return this.locks[(int)(value % this.locks.length)];
	}
	
}
