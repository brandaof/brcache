package org.brandao.brcache.memory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

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
	
	public byte get(long off){
		
		//if(off >= this.length){
		//	throw new IndexOutOfBoundsException(off + " >= " + this.length);
		//}
		
		return UnsafeMemoryUtil.getByte(this.address, off);
	}
	
	public int read(long thisOff, byte[] buf, int off, int len){
		
		int max = (int)(this.length - thisOff);
		max     = max > len? len : max;

		/*if(max > 0){
			
			if(thisOff + max > this.length){
				throw new IndexOutOfBoundsException((thisOff + max) + " > " + this.length);
			}*/
			
			UnsafeMemoryUtil.arrayCopy(this.address, thisOff, buf, off, max);
		//}
		
		return max;
	}
	
	public long read(long thisOff, RegionMemory buf, long off, long len){
		
		long max = (this.length - thisOff);
		max      = max > len? len : max;
		
		/*if(max > 0){
			
			if(thisOff + max > this.length){
				throw new IndexOutOfBoundsException("this: " + (thisOff + max) + " > " + this.length);
			}
			
			if(off + max > this.length){
				throw new IndexOutOfBoundsException("buf: " + (off + max) + " > " + this.length);
			}*/
			
			UnsafeMemoryUtil.arrayCopy(this.address, thisOff, ((UnsafeRegionMemory)buf).address, off, max);
		//}
		
		return max;
	}
	
	public void read(OutputStream out, int off, int len) throws IOException {
		byte[] tmp = new byte[2048];
		int read;
		int thisOff = off;
		int maxRead = len > tmp.length? tmp.length : len;
		while((read = read(thisOff, tmp, 0, maxRead)) > 0){
			out.write(tmp, 0, read);
			thisOff += read;
			len     -= read;
			maxRead  = len > tmp.length? tmp.length : len;
		}
	}
	
	public void write(long thisOff, byte[] buf, int off, int len){
		
		long max = (this.length - thisOff);
		max      = max > len? len : max;
		
		/*if(max > 0){
			if(thisOff + max > this.length){
				throw new IndexOutOfBoundsException((thisOff + max) + " > " + this.length);
			}*/
			
			UnsafeMemoryUtil.arrayCopy(buf, off, this.address, thisOff, max);
		//}
	}

	public void write(long thisOff, RegionMemory buf, long off, long len){
		
		long max = (this.length - thisOff);
		max      = max > len? len : max;
		
		/*if(max > 0){
		
			if(thisOff + max > this.length){
				throw new IndexOutOfBoundsException("this: " + (thisOff + max) + " > " + this.length);
			}
			
			if(off + max > this.length){
				throw new IndexOutOfBoundsException("buf: " + (off + max) + " > " + this.length);
			}*/
			
			UnsafeMemoryUtil.arrayCopy(((UnsafeRegionMemory)buf).address, off, this.address, thisOff, max);
		//}
		
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
