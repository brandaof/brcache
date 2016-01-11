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
    
    private byte[] buffer;
    
    public ByteArrayWrapper(long id, int segment, byte[] data){
        this.id      = id;
        this.segment = segment;
        this.buffer  = data;
    }

    public void writeTo(OutputStream out) throws IOException{
        out.write(this.buffer, 0, buffer.length);
    }

    public byte[] toByteArray(){
        return this.buffer;
    }

	public long getId() {
		return id;
	}

	public int getSegment() {
		return segment;
	}

	@Override
    public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(buffer);
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
            if (!Arrays.equals(buffer, other.buffer))
                    return false;
            return true;
    }

}
