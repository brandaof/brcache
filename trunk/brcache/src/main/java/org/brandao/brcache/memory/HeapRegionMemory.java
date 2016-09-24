package org.brandao.brcache.memory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class HeapRegionMemory implements RegionMemory{

	private static final long serialVersionUID = -8225524015808420667L;

	byte[][] segments;

	int segmentSize;
	
	long length;
	
	public HeapRegionMemory(byte[][] segments, int segmentSize, int length){
		this.segments    = segments;
		this.segmentSize = segmentSize;
		this.length      = length;
	}
	
	byte[][] getSegments(){
		return segments;
	}
	
	public long size(){
		return this.length;
	}
	
	public byte get(long offset){
		
		if(offset >= this.length){
			throw new IndexOutOfBoundsException(offset + " >= " + this.length);
		}
		
		int seg = (int)(offset / this.segmentSize); 
		int off = (int)(offset % this.segmentSize);
		
		return this.segments[seg][off];
	}

	public int read(long thisOff, byte[] buf, int off, int len){

		if(thisOff >= this.length)
			return -1;
		
		len = (int)(thisOff + len > this.length? this.length - thisOff : len);
		
		int thisSegmentId    = (int)(thisOff/this.segmentSize);
		byte[] thisCurrent   = this.segments[thisSegmentId];
		int thisSegmentLimit = thisCurrent.length;
		int thisSegmentOff   = (int)(thisOff%this.segmentSize);
		int read             = 0;
		int totalRead        = 0;
    	int maxOff           = off + len;
    	int needRead         = 0;
    	int maxRead          = 0;
		int maxThisLen       = 0;
		
    	while(off < maxOff && thisOff < this.length){

	    	if(thisSegmentOff >= thisSegmentLimit){
	    		thisSegmentId    = (int)(thisOff/this.segmentSize);
	    		thisCurrent      = this.segments[thisSegmentId];
	    		thisSegmentOff   = (int)(thisOff%this.segmentSize);
	    		thisSegmentLimit = thisCurrent.length;
	    	}
    		
	    	maxThisLen = (int)(this.length - thisOff);
	    	needRead   = maxOff - off;
	    	maxRead    = thisSegmentLimit - thisSegmentOff;
	    	
	    	if(needRead > maxThisLen)
	    		needRead = maxThisLen;
	    	
	    	if(needRead > maxRead)
	    		read = maxRead;
	    	else
	    		read = needRead;
    		
    		System.arraycopy(thisCurrent, thisSegmentOff, buf, off, read);

    		totalRead      += read;
    		thisSegmentOff += read;
	    	off            += read;
	    	thisOff        += read;
    		
    	}
    	
    	return totalRead;
	}
	
	public long read(long thisOff, RegionMemory b, long off, long len){
		HeapRegionMemory buf = (HeapRegionMemory)b; 
		if(thisOff >= this.length)
			return -1;
		
		int totalRead     = 0;
		byte[][] segments = buf.segments;
		int segmentSize   = buf.segmentSize;
		
		int thisSegmentId    = (int)(thisOff/this.segmentSize);
		byte[] thisCurrent   = this.segments[thisSegmentId];
		int thisSegmentLimit = thisCurrent.length;
		int thisSegmentOff   = (int)(thisOff%this.segmentSize);
		
		int segmentId    = (int)(off/segmentSize);
		byte[] current   = segments[segmentId];
		int segmentLimit = current.length;
		int segmentOff   = (int)(off%segmentSize);
		
    	long maxOff       = off + len;
    	
    	int transferLen   = 0;
    	int needRead      = 0;
    	int maxRead       = 0;
		int maxThisLen    = 0;
		int read          = 0;
				
		long maxLen       = 0;
    	int needWrite     = 0;
    	int maxWrite      = 0;
		int write         = 0;
		
    	while(off < maxOff){

	    	if(thisSegmentOff >= thisSegmentLimit){
	    		thisSegmentId    = (int)(thisOff/this.segmentSize);
	    		thisCurrent      = this.segments[thisSegmentId];
	    		thisSegmentOff   = (int)(thisOff%this.segmentSize);
	    		thisSegmentLimit = thisCurrent.length;
	    	}

	    	if(segmentOff >= segmentLimit){
	    		segmentId    = (int)(off/segmentSize);
	    		current      = segments[segmentId];
	    		segmentOff   = (int)(off % segmentSize);
	    		segmentLimit = current.length;
	    	}
	    	
	    	transferLen = (int)(maxOff - off);
	    	
	    	maxThisLen  = (int)(this.length - thisOff);
	    	needRead    = transferLen;
	    	maxRead     = thisSegmentLimit - thisSegmentOff;
	    	
	    	if(needRead > maxThisLen)
	    		needRead = maxThisLen;
	    	
	    	if(needRead > maxRead)
	    		read = maxRead;
	    	else
	    		read = needRead;

	    	maxLen    = buf.length - off;
	    	needWrite = transferLen;
	    	maxWrite  = segmentLimit - segmentOff;
	    	
	    	if(needWrite > maxLen)
	    		throw new IndexOutOfBoundsException(needWrite + " > " + maxLen);
	    	
	    	if(needWrite > maxWrite)
	    		write = maxWrite;
	    	else
	    		write = needWrite;
	    	
	    	int copy;
	    	
    		if(maxWrite > maxRead)
    			copy = read;
    		else
    			copy = write;

    		if(copy == 0)
    			break;
    		
    		System.arraycopy(thisCurrent, thisSegmentOff, current, segmentOff, copy);

    		thisOff        += copy;
    		off            += copy;
    		thisSegmentOff += copy;
    		segmentOff     += copy;
    		totalRead      += copy;
    	}
    	
    	return totalRead;
	}
	
	public void write(long thisOff, byte[] buf, int off, int len){

		if(len == 0)
			return;
		
		long maxThisOff = this.length;
		int maxOff      = off + len;

		if(maxOff > buf.length)
			throw new IndexOutOfBoundsException(maxOff + " > " + buf.length);
		
		int thisSegmentIndex = (int)(thisOff/this.segmentSize);
		int thisSegmentOff   = (int)(thisOff%this.segmentSize);
		byte[] thisSegment   = this.segments[thisSegmentIndex]; 
		int maxThisSegment   = thisSegment.length; 
		
    	while(off < maxOff){

	    	if(thisSegmentOff >= maxThisSegment){
	    		thisSegmentIndex = (int)(thisOff/this.segmentSize);
	    		thisSegmentOff   = (int)(thisOff%this.segmentSize);
	    		thisSegment      = this.segments[thisSegmentIndex]; 
	    		maxThisSegment   = thisSegment.length; 
	    	}
    		
	    	int maxRead   = maxOff - off;
	    	long maxWrite = maxThisOff - thisOff;
	    	
	    	if(maxRead > maxWrite)
	    		throw new IndexOutOfBoundsException(maxOff + " > " + maxThisOff);
	    	
	    	int maxSegmentWrite = maxThisSegment - thisSegmentOff;
	    	int copy;
	    	
	    	if(maxRead > maxSegmentWrite)
	    		copy = maxSegmentWrite;
	    	else
	    		copy = maxRead;

    		System.arraycopy(buf, off, thisSegment, thisSegmentOff, copy);
	    	
    		thisSegmentOff += copy;
    		off            += copy;
    		thisOff        += copy;
    	}
	}

	public void write(long thisOff, RegionMemory b, long off, long len){
		HeapRegionMemory buf = (HeapRegionMemory)b; 
		
		if(len == 0)
			return;
		
		long maxThisOff = this.length;
		long maxOff     = off + len;

		if(maxOff > buf.length)
			throw new IndexOutOfBoundsException(maxOff + " > " + buf.length);
		
		int thisSegmentIndex = (int)(thisOff/this.segmentSize);
		int thisSegmentOff   = (int)(thisOff%this.segmentSize);
		byte[] thisSegment   = this.segments[thisSegmentIndex]; 
		int maxThisSegment   = thisSegment.length; 

		int bufSegmentIndex = (int)(off/this.segmentSize);
		int bufSegmentOff   = (int)(off%this.segmentSize);
		byte[] bufSegment   = buf.segments[bufSegmentIndex]; 
		int maxBufSegment   = bufSegment.length; 
		
    	while(off < maxOff){

	    	if(thisSegmentOff >= maxThisSegment){
	    		thisSegmentIndex = (int)(thisOff/this.segmentSize);
	    		thisSegmentOff   = (int)(thisOff%this.segmentSize);
	    		thisSegment      = this.segments[thisSegmentIndex]; 
	    		maxThisSegment   = thisSegment.length; 
	    	}

	    	if(bufSegmentOff >= maxBufSegment){
	    		bufSegmentIndex = (int)(off/this.segmentSize);
	    		bufSegmentOff   = (int)(off%this.segmentSize);
	    		bufSegment      = buf.segments[bufSegmentIndex]; 
	    		maxBufSegment   = bufSegment.length; 
	    	}
	    	
	    	int maxRead   = (int)(maxOff - off);
	    	long maxWrite = maxThisOff - thisOff;
	    	
	    	if(maxRead > maxWrite)
	    		throw new IndexOutOfBoundsException(maxOff + " > " + maxThisOff);
	    	
	    	int maxSegmentWrite = maxThisSegment - thisSegmentOff;
	    	int copy;
	    	
	    	if(maxRead > maxSegmentWrite)
	    		copy = maxSegmentWrite;
	    	else
	    		copy = maxRead;

    		System.arraycopy(bufSegment, bufSegmentOff, thisSegment, thisSegmentOff, copy);
	    	
    		bufSegmentOff  += copy;
    		thisSegmentOff += copy;
    		off            += copy;
    		thisOff        += copy;
    	}

    	
	}
	
	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeLong(this.length);
		stream.writeInt(this.segmentSize);
		for(byte[] b: this.segments){
			stream.write(b);
		}
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
		this.length      = stream.readLong();
		this.segments    = HeapMemoryUtil.alloc(length);
		this.segmentSize = HeapMemoryUtil.segmentSize;
		int segmentSize  = stream.readInt();
		
		int len;
		int thisOff = 0;
		byte[] b    = new byte[segmentSize];
		
		while(thisOff < this.length){
			len = stream.read(b, 0, b.length);
			int maxLen = (int)Math.min(b.length, this.length - thisOff);
			this.write(thisOff, b, 0, maxLen);
			thisOff += len;
			if(len == 0){
				break;
			}
				
		}
    }
	
    protected void finalize() throws Throwable{
    	try{
    		HeapMemoryUtil.free(this.segments);
    	}
    	finally{
    		super.finalize();
    	}
    }

}
