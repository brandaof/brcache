package org.brandao.brcache.memory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class UnsafeRegionMemory 
	implements RegionMemory{

	private static final long serialVersionUID = 5941866368711530651L;

	Long address;

	long length;
	
	long off;
	
	public UnsafeRegionMemory(long address, Long length) {
		this.address = address;
		this.length  = length;
		this.off     = 0;
	}

	public void reset(){
		this.off = 0;
	}
	
	public long size(){
		return this.length;
	}
	
	public int read(byte[] buf, int off, int len){
		long trans = Math.min(this.length - this.off, len);
		long buffAdress = UnsafeMemoryUtil.getAddress(buf);
		UnsafeMemoryUtil.arrayCopy(this.address, this.off, buffAdress, off, trans);
		this.off += trans;
		
		return (int)trans;
	}
	
	public long read(RegionMemory buf, long off, long len){
		long trans = Math.min(this.length - this.off, len);
		UnsafeMemoryUtil.arrayCopy(this.address, this.off, ((UnsafeRegionMemory)buf).address, off, trans);
		this.off += trans;
		return trans;
	}
	
	public void write(byte[] buf, int off, int len){
		long buffAdress = UnsafeMemoryUtil.getAddress(buf);
		long trans = Math.min(this.length - this.off, len);
		UnsafeMemoryUtil.arrayCopy(buffAdress, off, this.address, this.off, trans);
		this.off += trans;
	}

	public void write(RegionMemory buf, long off, long len){
		long trans = Math.min(this.length - this.off, len);
		UnsafeMemoryUtil.arrayCopy(((UnsafeRegionMemory)buf).address, off, this.address, this.off, trans);
		this.off += trans;
	}
	
	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeLong(this.length);
		long len;
		byte[] b = new byte[9024];
		while((len = this.read(b, 0, b.length)) > 0){
			stream.write(b, 0, (int)len);
		}
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    	this.length  = stream.readLong();
    	this.off     = 0;
    	this.address = UnsafeMemoryUtil.alloc(length);
		int len;
		byte[] b = new byte[9024];
		while(this.off < this.length){
			int maxLen = (int)Math.min(b.length, this.length - this.off);
			len = stream.read(b, 0, maxLen);
			this.write(b, 0, len);
			if(len == 0){
				break;
			}
				
		}
		
		this.reset();
    }
	
    protected void finalize() throws Throwable{
    	try{
    		UnsafeMemoryUtil.free(this.address);
    	}
    	finally{
    		super.finalize();
    	}
    }

	public void setOffset(long value) {
		this.off = value;
	}

}
