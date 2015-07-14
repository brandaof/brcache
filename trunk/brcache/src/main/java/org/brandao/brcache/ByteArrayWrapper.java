package org.brandao.brcache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

@SuppressWarnings("serial")
public class ByteArrayWrapper 
	implements Serializable{

	private transient ByteBuffer buffer;
	
	public ByteArrayWrapper(byte[] data){
		this.buffer = ByteBuffer.allocateDirect(data.length);
		this.buffer.put(data);
		this.buffer.flip();
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
    	out.writeInt(data.length);
    	out.write(data, 0, data.length);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    	int length = in.readInt();
    	byte[] data = new byte[length];
    	in.read(data, 0, data.length);
		this.buffer = ByteBuffer.allocateDirect(data.length);
		this.buffer.put(data);
		this.buffer.flip();
    }
	
}
