package org.brandao.brcache.memory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

class RegionMemory implements Serializable{

	Long address;

	long length;
	
	public RegionMemory(long address, Long length) {
		this.address = address;
		this.length = length;
	}

	public long length(){
		return this.length;
	}
	
	public int read(int thisOff, byte[] buf, long off, long len){
		return -1;
	}
	
	public int read(int thisOff, RegionMemory buf, long off, long len){
		return -1;
	}

	public int write(InputStream stream, long off, long len) throws IOException{
		
		if(off >= length)
			throw new IndexOutOfBoundsException("off: " + off + ">=" + this.length);

		byte[] buffer   = new byte[2048];
		long bufferAddr = Memory.getAddress(buffer);
		int read        = 0;
		len             = off + len > this.length? this.length - off : len;
		
		while(len > 0){
			
			int r = stream.read(buffer, 0, buffer.length);
			
			if(r < 0){
				return -1;
			}
			
			//Memory.copy(bufferAddr, 0, address, off, r);
			read += r;
			len  -= r;
		}
		return read;
	}
	
	public void write(int thisOff, byte[] buf, long off, long len){
		if(thisOff >= length)
			throw new IndexOutOfBoundsException("thisOff: " + thisOff + ">=" + this.length);
		
		if(off >= buf.length)
			throw new IndexOutOfBoundsException("thisOff: " + off + ">=" + buf.length);
		
		//int maxRead = thisOff + len > this.length?
	}

	public void write(OutputStream stream, long off, long len) throws IOException{
	}

	public void write(int thisOff, RegionMemory buf, long off, int len){
	}
	
	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeLong(this.length);
		this.write(stream, 0, this.length);
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    	long length = stream.readLong();
		Memory.alloc(length, this);
		this.write(stream, 0, length);
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
