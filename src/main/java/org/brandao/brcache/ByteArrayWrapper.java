package org.brandao.brcache;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

@SuppressWarnings("serial")
public class ByteArrayWrapper 
    implements Serializable{

    private long id;

    private int segment;
    
    private int length;
    
    private RegionMemory buffer;
    
    public ByteArrayWrapper(long id, int segment, RegionMemory data, int length){
        this.id      = id;
        this.segment = segment;
        this.buffer  = data;
        this.length  = length;
    }

    public void writeTo(OutputStream out) throws IOException{
    	this.buffer.write(out, 0, length);
    }

    public RegionMemory toByteArray(){
        return this.buffer;
    }

	public long getId() {
		return id;
	}

	public int getSegment() {
		return segment;
	}

	public int getLength() {
		return length;
	}

	@Override
    public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(buffer.getSegments());
            return result;
    }

    @Override
    public boolean equals(Object obj) {
            if (this == obj)
                    return true;
            if (obj == null)
                    return false;
            if (getClass() != obj.getClass())
                    return false;
            ByteArrayWrapper other = (ByteArrayWrapper) obj;
            if (!Arrays.equals(buffer.getSegments(), other.buffer.getSegments()))
                    return false;
            return true;
    }

}
