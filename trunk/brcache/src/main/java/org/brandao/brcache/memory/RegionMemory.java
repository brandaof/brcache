package org.brandao.brcache.memory;

import java.io.Serializable;

public interface RegionMemory extends Serializable{

	void setOffset(long value);
	
	void reset();
	
	long size();
	
	int read(byte[] buf, int off, int len);
	
	long read(RegionMemory buf, long off, long len);
	
	void write(byte[] buf, int off, int len);

	void write(RegionMemory buf, long off, long len);
	
}
