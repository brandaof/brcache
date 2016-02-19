package org.brandao.brcache.collections.fileswapper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataBlockEntityFile 
	extends AbstractEntityFile<DataBlock>{

	public static final byte LEN_HEADER 		= 11;
	
	public static final byte END_HEADER 		= 0x0D;

	public static final byte EOF_FILE   		= 26;

	public static final byte REMOVED_STATUS     = 0x0d;

	public static final byte USED_STATUS        = 0x1f;
	
	private final int blockSize;

	private ReadWriteLock lock;
	
	public DataBlockEntityFile(File file, int blockSize) {
		super(file);
		this.firstRecord	= LEN_HEADER;
		this.blockSize		= blockSize;
		this.recordLength	= this.blockSize + 13;
		this.lock			= new ReentrantReadWriteLock();
	}

	@Override
	protected void writeEOF(DataOutputStream stream) throws IOException {
		stream.writeByte(EOF_FILE);
	}

	@Override
	protected void write(DataOutputStream stream, DataBlock entity)
			throws IOException {
		
		byte[] buffer = new byte[this.blockSize];
		
		if(entity != null){
			
			byte[] data = entity.getData();
			
			int length = data.length;
			
			if(length > this.blockSize)
				throw new IOException(length + " != " + this.blockSize);
			
			System.arraycopy(data, 0, buffer, 0, length);
			
			stream.writeByte(USED_STATUS);
			stream.writeInt(length);
			stream.writeLong(entity.getNextBlock());
			stream.write(buffer, 0, buffer.length);
		}
		else{
			stream.writeByte(REMOVED_STATUS);
			stream.writeInt(-1);
			stream.writeLong(-1);
			stream.write(buffer, 0, buffer.length);
		}
		
	}

	@Override
	protected DataBlock read(DataInputStream stream) throws IOException {
		
		byte status = stream.readByte();
		
		if(status == REMOVED_STATUS)
			return null;

		int length = stream.readInt();
		long nextBlock = stream.readLong();
		byte[] data = new byte[length];
		stream.read(data, 0, data.length);

		DataBlock entity = new DataBlock();
		entity.setData(data);
		entity.setNextBlock(nextBlock);
		entity.setId(this.offset);
		
		return entity;
	}


	@Override
	protected void writeMetaData(DataOutputStream stream) throws IOException {
		stream.writeString("DTA", 6);
		stream.writeShort((short)this.firstRecord);
		stream.writeShort((short)this.recordLength);
		stream.writeByte((byte)END_HEADER);
	}

	protected void writeHeader() throws IOException{
		super.writeHeader();
		DataBlock first = new DataBlock();
		first.setNextBlock(-1);
		first.setData(new byte[0]);
		this.write(first);
	}

	@Override
	protected void readMetaData(DataInputStream stream) throws IOException {
		String name = stream.readString(6);
		
		if(!"DTA".equals(name))
			throw new IOException("invalid file format");
		
		this.firstRecord  = stream.readShort();
		this.recordLength = stream.readShort();
		
		byte end = stream.readByte();
		
		if(end != END_HEADER)
			throw new IOException("header corrupted");
	}

	public ReadWriteLock getLock() {
		return this.lock;
	}

	public int getBlockSize() {
		return blockSize;
	}

}
