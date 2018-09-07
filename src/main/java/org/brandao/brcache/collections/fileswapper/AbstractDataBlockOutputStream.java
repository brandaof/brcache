package org.brandao.brcache.collections.fileswapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public abstract class AbstractDataBlockOutputStream 
	extends OutputStream{

    private int offset;
    
    private byte[] buffer;
	
    private int capacity;
    
    public AbstractDataBlockOutputStream(int blockSize){
        this.offset    = 0;
        this.capacity  = blockSize;
        this.buffer    = new byte[capacity];
    }
    
    public void write(byte[] buffer, int offset, int len) throws IOException{
    	
        int maxDesloc = offset + len;
        
        if(this.offset == this.capacity)
        	this.flush();
        
        while(offset != maxDesloc){
            int maxRead  = maxDesloc - offset;
            int maxWrite = this.capacity - this.offset;
            
            if(maxRead > maxWrite){
                System.arraycopy(buffer, offset, this.buffer, this.offset, maxWrite);
                offset += maxWrite;
                this.offset += maxWrite;
                this.flush();
            }
            else{
                System.arraycopy(buffer, offset, this.buffer, this.offset, maxRead);
                offset      += maxRead;
                this.offset += maxRead;
            }
        }
        
    }
	
	public void write(int b) throws IOException {
        this.write(new byte[]{(byte)(b & 0xff)}, 0, 1);
    }

    public void flush() throws IOException{
    	if(this.offset > 0){
	    	byte[] block = Arrays.copyOf(this.buffer, this.offset);
	    	DataBlock dataBlock = new DataBlock();
	    	dataBlock.setData(block);
	    	addBlock(dataBlock);
	        this.offset = 0;
    	}
    }
	
    public void close() throws IOException{
    	this.flush();
    }

    protected abstract void addBlock(DataBlock dataBlock);
    
}
