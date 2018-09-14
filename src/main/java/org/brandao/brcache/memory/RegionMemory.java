package org.brandao.brcache.memory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

public interface RegionMemory extends Serializable{

	long size();
	
	int read(long thisOff, byte[] buf, int off, int len);
	
	long read(long thisOff, RegionMemory buf, long off, long len);
	
	void read(OutputStream out, int off, int len) throws IOException;
	
	void write(long thisOff, byte[] buf, int off, int len);

	void write(long thisOff, RegionMemory buf, long off, long len);
	
	byte get(long off);
	
}
