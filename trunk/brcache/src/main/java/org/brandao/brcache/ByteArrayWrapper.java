package org.brandao.brcache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

@SuppressWarnings("serial")
public class ByteArrayWrapper 
	implements Serializable{

	private byte[] buffer;
	
	public ByteArrayWrapper(byte[] data){
		this.buffer = data;
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
	
	/*
	private transient ByteBuffer buffer;
	
	public ByteArrayWrapper(byte[] data){
		this.buffer = ByteBuffer.allocateDirect(data.length);
		this.buffer.put(data, 0, data.length);
		this.buffer.limit(data.length);
	}

	public synchronized void writeTo(OutputStream out) throws IOException{
		this.buffer.position(0);
		WritableByteChannel c = Channels.newChannel(out);
		c.write(this.buffer);
	}
	
	public synchronized byte[] toByteArray(){
        byte[] r = new byte[this.buffer.limit()];
		this.buffer.position(0);
        this.buffer.get(r, 0, r.length);
		return r;
	}
	
    private void writeObject(ObjectOutputStream out) throws IOException {
        byte[] data = new byte[this.buffer.limit()];
		this.buffer.position(0);
        this.buffer.get(data, 0, data.length);
        this.buffer.clear();
        out.writeObject(data);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    	byte[] data = (byte[]) in.readObject();
		this.buffer = ByteBuffer.allocateDirect(data.length);
		this.buffer.put(data, 0, data.length);
		this.buffer.limit(data.length);
    }
*/
	
}
