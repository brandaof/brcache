package org.brandao.brcache.memory;

import java.io.Serializable;

public interface RegionMemory extends Serializable{

	void reset();
	
	long size();
	
	long read(byte[] buf, long off, long len);
	
	long read(RegionMemory buf, long off, long len);
	
	void write(byte[] buf, long off, long len);

	void write(RegionMemory buf, long off, long len);
	
}
