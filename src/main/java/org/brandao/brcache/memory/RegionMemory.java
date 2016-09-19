package org.brandao.brcache.memory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class RegionMemory implements Serializable{

	private static final long serialVersionUID = 5941866368711530651L;

	Long address;

	long length;
	
	long off;
	
	public RegionMemory(long address, Long length) {
		this.address = address;
		this.length  = length;
		this.off     = 0;
	}

	public void reset(){
		this.off = 0;
	}
	
	public long length(){
		return this.length;
	}
	
	public long read(byte[] buf, long off, long len){
		long trans = Math.min(this.length - this.off, len);
		long buffAdress = Memory.getAddress(buf);
		Memory.arrayCopy(this.address, this.off, buffAdress, off, trans);
		this.off += trans;
		return trans;
	}
	
	public long read(RegionMemory buf, long off, long len){
		long trans = Math.min(this.length - this.off, len);
		Memory.arrayCopy(this.address, this.off, buf.address, off, trans);
		this.off += trans;
		return trans;
	}
	
	public void write(byte[] buf, long off, long len){
		long buffAdress = Memory.getAddress(buf);
		long trans = Math.min(this.length - this.off, len);
		Memory.arrayCopy(buffAdress, off, this.address, this.off, trans);
		this.off += trans;
	}

	public void write(RegionMemory buf, long off, long len){
		long trans = Math.min(this.length - this.off, len);
		Memory.arrayCopy(buf.address, off, this.address, this.off, trans);
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
    	this.length = stream.readLong();
    	this.off    = 0;
		Memory.alloc(length, this);
		long len;
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
    		Memory.release(this);
    	}
    	finally{
    		super.finalize();
    	}
    }
}
