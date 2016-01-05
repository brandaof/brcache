
package org.brandao.brcache.collections.fileswapper;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Brandao
 */
public class DataBlockInputStream extends InputStream{
    
    private int offset;
    
    private int limit;
    
    private byte[] buffer;
    
    private DataBlockEntityFile dataBlockEntityFile;

    private DataBlock currentDataBlock;
    
    private long id;
    
    public DataBlockInputStream(long id, DataBlockEntityFile dataBlockEntityFile) throws IOException{
        this.id       = id;
        this.offset   = 0;
        this.limit    = 0;
        this.dataBlockEntityFile = dataBlockEntityFile;
        this.currentDataBlock    = null;
    }

    public int read(byte[] buffer, int offset, int len) throws IOException{
    	
    	int totalRead = 0;
    	int limit     = offset + len;
    	
    	while(offset < limit){

	    	if(this.offset == this.limit){
	    		if(this.currentDataBlock == null){
	    	        this.dataBlockEntityFile.seek(this.id);
	    	        this.currentDataBlock = this.dataBlockEntityFile.read();
	    		}
	    		else
	    		if(this.currentDataBlock.getNextBlock() != -1){
	    	        this.dataBlockEntityFile.seek(this.currentDataBlock.getNextBlock());
	    	        this.currentDataBlock = this.dataBlockEntityFile.read();
	    		}
	    		else
	    			this.currentDataBlock = null;

	    		if(this.currentDataBlock == null)
	    			return -1;
	    		
	    		this.buffer = this.currentDataBlock.getData();
    	        this.limit  = this.buffer.length;
	    		this.offset = 0;
	    	}
    		
	    	int needRead = limit - offset;
	    	int maxRead  = this.limit - this.offset;
	    	int read;
	    	
    		if(needRead > maxRead)
    			read = maxRead;
    		else
    			read = needRead;
    		
    		System.arraycopy(this.buffer, this.offset, buffer, offset, read);
    		
    		offset      += read;
    		this.offset += read;
    		totalRead   += read;
    	}
    	
   		return totalRead;
    }

	@Override
	public int read() throws IOException {
		byte[] buffer = new byte[1];
		int read = read(buffer, 0, 1);
		return read < 1? -1 : buffer[0] & 0xff;
	}
	
	public boolean markSupported() {
		return false;
	}
	
}
