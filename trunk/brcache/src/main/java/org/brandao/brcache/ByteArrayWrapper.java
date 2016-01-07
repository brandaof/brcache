package org.brandao.brcache;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

@SuppressWarnings("serial")
public class ByteArrayWrapper 
	implements Serializable{

	private byte[] buffer;
	
        private String id;
        
	public ByteArrayWrapper(String id, byte[] data){
		this.buffer = data;
                this.id = id;
	}

	public synchronized void writeTo(OutputStream out) throws IOException{
		out.write(this.buffer, 0, buffer.length);
	}
	
	public synchronized byte[] toByteArray(){
		return this.buffer;
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

    public String getId() {
        return id;
    }
	
}
