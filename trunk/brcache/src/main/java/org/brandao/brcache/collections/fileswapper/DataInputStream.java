package org.brandao.brcache.collections.fileswapper;

import java.io.IOException;
import java.io.InputStream;

public class DataInputStream {

	private byte[] buffer = new byte[8];
	
	private InputStream stream;
	
	public DataInputStream(InputStream stream){
		this.stream = stream;
	}
	
	public long readLong() throws IOException {
		this.stream.read(buffer, 0, 8);

		return 
			 (long)buffer[0]       & 0xffL              | 
			((long)buffer[1] << 8  & 0xff00L)           | 
			((long)buffer[2] << 16 & 0xff0000L)         | 
			((long)buffer[3] << 24 & 0xff000000L)       |
			((long)buffer[4] << 32 & 0xff00000000L)     | 
			((long)buffer[5] << 40 & 0xff0000000000L)   | 
			((long)buffer[6] << 48 & 0xff000000000000L) | 
			((long)buffer[7] << 56 & 0xff00000000000000L);
	}

	public int readInt() throws IOException {
		this.stream.read(buffer, 0, 4);
		return 
			 (int)buffer[0]       & 0xff      | 
			((int)buffer[1] << 8  & 0xff00)   | 
			((int)buffer[2] << 16 & 0xff0000) | 
			((int)buffer[3] << 24 & 0xff000000);
	}

	public short readShort() throws IOException {
		this.stream.read(buffer, 0, 2);
		return (short)(
				 (short)buffer[0]       & 0xffL |
				((short)buffer[1] << 8  & 0xff00)
		);
	}

	public byte readByte() throws IOException {
		this.stream.read(buffer, 0, 1);
		return buffer[0];
	}

	public char readChar() throws IOException {
		this.stream.read(buffer, 0, 1);
		return (char) buffer[0];
	}
	
	public int read(byte[] b) throws IOException{
		return this.stream.read(b);
	}
	
	public int read(byte[] b, int off, int len) throws IOException{
		return this.stream.read(b, off, len);
	}
	
	public String readString(int length) throws IOException{
		byte[] value = new byte[length];
		this.stream.read(value, 0, length);
		
		
		short size = (short)(
				 value[0]       & 0xffL |
				(value[1] << 8  & 0xff00)
		);

		if(size == -1)
			return null;
		
		int init = 2;
		int end  = init + size;
		
		if(end > value.length)
			throw new IOException(end + " > " + value.length);

		return new String(value, init, size, "UTF-8");
	}

}
