package org.brandao.brcache.collections.fileswapper;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileAccess {

	private byte[] buffer = new byte[8];
	
	private RandomAccessFile randomAccessFile;
	
	private File file;
	
	public FileAccess(File file, RandomAccessFile randomAccessFile){
		this.randomAccessFile = randomAccessFile;
		this.file = file;
	}

	public long readLong() throws IOException {
		this.randomAccessFile.readFully(buffer, 0, 8);
		return 
			buffer[0] | (buffer[1] << 8) | (buffer[2] << 16) | (buffer[3] << 24) |
			(buffer[4] << 32) | (buffer[5] << 40) | (buffer[6] << 48) | (buffer[7] << 56);
	}

	public void writeLong(long value) throws IOException{
		buffer[0] = (byte)(value & 0xff);
		buffer[1] = (byte)((value >>  8) & 0xff);
		buffer[2] = (byte)((value >> 16) & 0xff);
		buffer[3] = (byte)((value >> 24) & 0xff);
		buffer[4] = (byte)((value >> 32) & 0xff);
		buffer[5] = (byte)((value >> 40) & 0xff);
		buffer[6] = (byte)((value >> 48) & 0xff);
		buffer[7] = (byte)((value >> 56) & 0xff);
		this.randomAccessFile.write(this.buffer, 0, 8);
	}
	
	public int readInt() throws IOException {
		this.randomAccessFile.readFully(buffer, 0, 4);
		return buffer[0] | (buffer[1] << 8) | (buffer[2] << 16) | (buffer[3] << 24);
	}

	public void writeInt(int value) throws IOException{
		buffer[0] = (byte)(value & 0xff);
		buffer[1] = (byte)((value >> 8) & 0xff);
		buffer[2] = (byte)((value >> 16) & 0xff);
		buffer[3] = (byte)((value >> 24) & 0xff);
		this.randomAccessFile.write(this.buffer, 0, 4);
	}

	public short readShort() throws IOException {
		this.randomAccessFile.readFully(buffer, 0, 2);
		return (short)(buffer[0] | (buffer[1] << 8));
	}

	public void writeShort(short value) throws IOException{
		buffer[0] = (byte)(value & 0xff);
		buffer[1] = (byte)((value >> 8) & 0xff);
		this.randomAccessFile.write(this.buffer, 0, 2);
	}

	public byte readByte() throws IOException {
		this.randomAccessFile.readFully(buffer, 0, 1);
		return buffer[0];
	}

	public void writeByte(byte value) throws IOException{
		buffer[0] = value;
		this.randomAccessFile.write(this.buffer, 0, 1);
	}
	
	public char readChar() throws IOException {
		this.randomAccessFile.readFully(buffer, 0, 1);
		return (char)buffer[0];
	}
	
	public void writeChar(char value) throws IOException{
		buffer[0] = (byte)value;
		this.randomAccessFile.write(this.buffer, 0, 1);
	}
	
	public String readString(int length) throws IOException{
		byte[] result = new byte[length];
		this.randomAccessFile.read(result, 0, length);

		int start = 0;
		int end = 0;
		for(int i=result.length-1;i>=0;i--){
			if(result[i] != ' '){
				end = i+1;
				break;
			}
		}
		
		return new String(result, start, end);
	}
	
	public void writeString(String value, int length) throws IOException{
		byte[] result = new byte[length];
		int start = 0;
		
		if(value != null){
			byte[] data = value.getBytes();
			System.arraycopy(data, 0, result, 0, data.length);
			start = data.length;
		}
		
		for(int i=start;i<result.length;i++){
			result[i] = ' ';
		}
		
		this.randomAccessFile.write(result, 0, length);
	}
	
	public void seek(long pos) throws IOException{
		this.randomAccessFile.seek(pos);
	}
	
	public int read(byte[] b) throws IOException{
		return this.randomAccessFile.read(b);
	}
	
	public int read(byte[] b, int off, int len) throws IOException{
		return this.randomAccessFile.read(b, off, len);
	}
	
	public void write(byte[] b) throws IOException{
		this.randomAccessFile.write(b);
	}
	
	public void write(byte[] b, int off, int len) throws IOException{
		this.randomAccessFile.write(b, off, len);
	}
	
	public long getFilePointer() throws IOException{
		return this.randomAccessFile.getFilePointer();
	}

	public long length() throws IOException{
		return this.randomAccessFile.length();
	}

	public void setLength(long value) throws IOException{
		this.randomAccessFile.setLength(value);
	}
	public void close() throws IOException{
		this.randomAccessFile.close();
	}
	
	public RandomAccessFile getRandomAccessFile(){
		return this.randomAccessFile;
	}
	
	public File getFile(){
		return this.file;
	}
	
}
