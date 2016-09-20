package org.brandao.brcache;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

class HeapRegionMemory implements Serializable{

	private static final long serialVersionUID = -8225524015808420667L;

	byte[][] segments;

	int segmentSize;
	
	int length;
	
	public HeapRegionMemory(byte[][] segments, int segmentSize, int length){
		this.segments    = segments;
		this.segmentSize = segmentSize;
		this.length      = length;
	}
	
	byte[][] getSegments(){
		return segments;
	}
	
	public int length(){
		return this.length;
	}
	
	public int read(InputStream stream, int off, int len) throws IOException{
		
		if(off + len > this.length)
			throw new IndexOutOfBoundsException((off + len) + " > " + this.length);
		
		if(len == 0)
			return 0;
		
		int thisSegmentId    = off/this.segmentSize;
		byte[] thisCurrent   = this.segments[thisSegmentId];
		int thisSegmentLimit = thisCurrent.length;
		int thisSegmentOff   = off%this.segmentSize;
		int maxOff           = off + len;
    	int needRead         = maxOff - off;
    	int maxRead          = thisSegmentLimit - thisSegmentOff;
		int totalRead        = 0;
    	int read             = 0;
    	
    	if(needRead > maxRead)
    		read = maxRead;
    	else
    		read = needRead;
    	
    	while(off < maxOff && (read = stream.read(thisCurrent, thisSegmentOff, read)) != -1){

	    	if(thisSegmentOff >= thisSegmentLimit){
	    		thisSegmentId    = off/this.segmentSize;
	    		thisCurrent      = this.segments[thisSegmentId];
	    		thisSegmentOff   = off%this.segmentSize;
	    		thisSegmentLimit = thisCurrent.length;
	    	}

    		thisSegmentOff += read;
    		totalRead      += read;
	    	off            += read;
	    	
	    	needRead = maxOff - off;
	    	maxRead  = thisSegmentLimit - thisSegmentOff;
	    	
	    	if(needRead > maxRead)
	    		read = maxRead;
	    	else
	    		read = needRead;
	    	
    	}
    	
    	return read == -1 && totalRead == 0? -1 : totalRead;
	}
	
	public int read(int thisOff, byte[] buf, int off, int len){

		if(thisOff >= this.length)
			return -1;
		
		len = thisOff + len > this.length? this.length - thisOff : len;
		
		int thisSegmentId    = thisOff/this.segmentSize;
		byte[] thisCurrent   = this.segments[thisSegmentId];
		int thisSegmentLimit = thisCurrent.length;
		int thisSegmentOff   = thisOff%this.segmentSize;
		int read             = 0;
		int totalRead        = 0;
    	int maxOff           = off + len;
    	int needRead         = 0;
    	int maxRead          = 0;
		int maxThisLen       = 0;
		
    	while(off < maxOff && thisOff < this.length){

	    	if(thisSegmentOff >= thisSegmentLimit){
	    		thisSegmentId    = thisOff/this.segmentSize;
	    		thisCurrent      = this.segments[thisSegmentId];
	    		thisSegmentOff   = thisOff%this.segmentSize;
	    		thisSegmentLimit = thisCurrent.length;
	    	}
    		
	    	maxThisLen = this.length - thisOff;
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
	
	public int read(int thisOff, HeapRegionMemory buf, int off, int len){
		
		if(thisOff >= this.length)
			return -1;
		
		int totalRead     = 0;
		byte[][] segments = buf.segments;
		int segmentSize   = buf.segmentSize;
		
		int thisSegmentId    = thisOff/this.segmentSize;
		byte[] thisCurrent   = this.segments[thisSegmentId];
		int thisSegmentLimit = thisCurrent.length;
		int thisSegmentOff   = thisOff%this.segmentSize;
		
		int segmentId    = off/segmentSize;
		byte[] current   = segments[segmentId];
		int segmentLimit = current.length;
		int segmentOff   = off%segmentSize;
		
    	int maxOff        = off + len;
    	
    	int transferLen   = 0;
    	int needRead      = 0;
    	int maxRead       = 0;
		int maxThisLen    = 0;
		int read          = 0;
				
		int maxLen        = 0;
    	int needWrite     = 0;
    	int maxWrite      = 0;
		int write         = 0;
		
    	while(off < maxOff){

	    	if(thisSegmentOff >= thisSegmentLimit){
	    		thisSegmentId    = thisOff/this.segmentSize;
	    		thisCurrent      = this.segments[thisSegmentId];
	    		thisSegmentOff   = thisOff%this.segmentSize;
	    		thisSegmentLimit = thisCurrent.length;
	    	}

	    	if(segmentOff >= segmentLimit){
	    		segmentId    = off/segmentSize;
	    		current      = segments[segmentId];
	    		segmentOff   = off % segmentSize;
	    		segmentLimit = current.length;
	    	}
	    	
	    	transferLen = maxOff - off;
	    	
	    	maxThisLen  = this.length - thisOff;
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
	
	public void write(int thisOff, byte[] buf, int off, int len){

		if(len == 0)
			return;
		
		int maxThisOff = this.length;
		int maxOff     = off + len;

		if(maxOff > buf.length)
			throw new IndexOutOfBoundsException(maxOff + " > " + buf.length);
		
		int thisSegmentIndex = thisOff/this.segmentSize;
		int thisSegmentOff   = thisOff%this.segmentSize;
		byte[] thisSegment   = this.segments[thisSegmentIndex]; 
		int maxThisSegment   = thisSegment.length; 
		
    	while(off < maxOff){

	    	if(thisSegmentOff >= maxThisSegment){
	    		thisSegmentIndex = thisOff/this.segmentSize;
	    		thisSegmentOff   = thisOff%this.segmentSize;
	    		thisSegment      = this.segments[thisSegmentIndex]; 
	    		maxThisSegment   = thisSegment.length; 
	    	}
    		
	    	int maxRead   = maxOff - off;
	    	int maxWrite  = maxThisOff - thisOff;
	    	
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

	public void write(OutputStream stream, int off, int len) throws IOException{
		
		if(len == 0)
			return;
		
		int maxOff     = off + len;

		if(maxOff > this.length)
			throw new IndexOutOfBoundsException(maxOff + " > " + this.length);
		
		int thisSegmentIndex = off/this.segmentSize;
		int thisSegmentOff   = off%this.segmentSize;
		byte[] thisSegment   = this.segments[thisSegmentIndex]; 
		int maxThisSegment   = thisSegment.length; 
		
    	while(off < maxOff){

	    	if(thisSegmentOff >= maxThisSegment){
	    		thisSegmentIndex = off/this.segmentSize;
	    		thisSegmentOff   = off%this.segmentSize;
	    		thisSegment      = this.segments[thisSegmentIndex]; 
	    		maxThisSegment   = thisSegment.length; 
	    	}

	    	int maxWrite = maxOff - off;
	    	int maxSegmentWrite = maxThisSegment - thisSegmentOff;
	    	
	    	int copy;
	    	
	    	if(maxWrite > maxSegmentWrite)
	    		copy = maxSegmentWrite;
	    	else
	    		copy = maxWrite;
	    	
	    	stream.write(thisSegment, thisSegmentOff, copy);
	    	
    		thisSegmentOff += copy;
	    	off            += copy;
	    	
    	}
    	
	}

	public void write(int thisOff, HeapRegionMemory buf, int off, int len){
		
		if(len == 0)
			return;
		
		int maxThisOff = this.length;
		int maxOff     = off + len;

		if(maxOff > buf.length)
			throw new IndexOutOfBoundsException(maxOff + " > " + buf.length);
		
		int thisSegmentIndex = thisOff/this.segmentSize;
		int thisSegmentOff   = thisOff%this.segmentSize;
		byte[] thisSegment   = this.segments[thisSegmentIndex]; 
		int maxThisSegment   = thisSegment.length; 

		int bufSegmentIndex = off/this.segmentSize;
		int bufSegmentOff   = off%this.segmentSize;
		byte[] bufSegment   = buf.segments[bufSegmentIndex]; 
		int maxBufSegment   = bufSegment.length; 
		
    	while(off < maxOff){

	    	if(thisSegmentOff >= maxThisSegment){
	    		thisSegmentIndex = thisOff/this.segmentSize;
	    		thisSegmentOff   = thisOff%this.segmentSize;
	    		thisSegment      = this.segments[thisSegmentIndex]; 
	    		maxThisSegment   = thisSegment.length; 
	    	}

	    	if(bufSegmentOff >= maxBufSegment){
	    		bufSegmentIndex = off/this.segmentSize;
	    		bufSegmentOff   = off%this.segmentSize;
	    		bufSegment      = buf.segments[bufSegmentIndex]; 
	    		maxBufSegment   = bufSegment.length; 
	    	}
	    	
	    	int maxRead   = maxOff - off;
	    	int maxWrite  = maxThisOff - thisOff;
	    	
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
		stream.writeInt(this.length);
		this.write(stream, 0, this.length);
		//Release ocorre no método finalize
		//Memory.release(this);
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    	int length = stream.readInt();
		HeapMemory.alloc(length, this);
		this.read(stream, 0, length);
    }
	
    protected void finalize() throws Throwable{
    	try{
    		HeapMemory.release(this);
    	}
    	finally{
    		super.finalize();
    	}
    }
}
