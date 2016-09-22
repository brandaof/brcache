package org.brandao.brcache.memory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class UnsafeRegionMemory 
	implements RegionMemory{

	private static final long serialVersionUID = 5941866368711530651L;

	Long address;

	long length;
	
	public UnsafeRegionMemory(long address, Long length) {
		this.address = address;
		this.length  = length;
	}

	public long size(){
		return this.length;
	}
	
	public int read(long thisOff, byte[] buf, int off, int len){
		long trans = Math.min(this.length - thisOff, len);
		UnsafeMemoryUtil.arrayCopy(this.address, thisOff, buf, off, trans);
		return (int)trans;
	}
	
	public long read(long thisOff, RegionMemory buf, long off, long len){
		long trans = Math.min(this.length - thisOff, len);
		UnsafeMemoryUtil.arrayCopy(this.address, thisOff, ((UnsafeRegionMemory)buf).address, off, trans);
		return trans;
	}
	
	public void write(long thisOff, byte[] buf, int off, int len){
		
		if(thisOff + len > this.length)
			throw new IndexOutOfBoundsException(thisOff + len + " >= " + this.length);
		
		long trans = Math.min(this.length - thisOff, len);
		UnsafeMemoryUtil.arrayCopy(buf, off, this.address, thisOff, trans);
	}

	public void write(long thisOff, RegionMemory buf, long off, long len){
		
		if(thisOff + len > this.length)
			throw new IndexOutOfBoundsException("this:" + thisOff + len + " > " + thisOff + this.length);

		if(off + len > buf.size())
			throw new IndexOutOfBoundsException("buf:" + off + len + " > " + buf.size());
		
		long trans = Math.min(this.length - thisOff, len);
		UnsafeMemoryUtil.arrayCopy(((UnsafeRegionMemory)buf).address, off, this.address, thisOff, trans);
	}
	
	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeLong(this.length);
		long len;
		byte[] b = new byte[9024];
		int off = 0;
		while((len = this.read(off, b, 0, b.length)) > 0){
			stream.write(b, 0, (int)len);
			off += len;
		}
		
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    	this.length  = stream.readLong();
    	this.address = UnsafeMemoryUtil.alloc(length);
		int len;
		int thisOff = 0;
		byte[] b = new byte[9024];
		while(thisOff < this.length){
			int maxLen = (int)Math.min(b.length, this.length - thisOff);
			len = stream.read(b, 0, maxLen);
			this.write(thisOff, b, 0, len);
			thisOff += len;
			if(len == 0){
				break;
			}
				
		}
    }
	
    protected void finalize() throws Throwable{
    	try{
    		synchronized(this){
    			if(this.address != null){
	    			UnsafeMemoryUtil.free(this.address);
	    			this.address = null;
    			}
    		}
    	}
    	finally{
    		super.finalize();
    	}
    }

}
