package org.brandao.brcache.collections.fileswapper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleIndexEntityFile 
	extends AbstractEntityFile<IndexKey>{

	public static final byte LEN_HEADER = 11;
	
	public static final byte END_HEADER = 0x0D;

	public static final byte EOF_FILE   = 26;
	
	private ReadWriteLock lock;
	
	public SimpleIndexEntityFile(File file) {
		super(file);
		this.firstRecord  = LEN_HEADER;
		this.recordLength = IndexKey.DATA_SIZE;
		this.lock         = new ReentrantReadWriteLock();
	}

	@Override
	protected void writeEOF(DataOutputStream stream) throws IOException {
		stream.writeByte(EOF_FILE);
	}

	@Override
	protected void write(DataOutputStream stream, IndexKey entity)
			throws IOException {
		
		long[] page = entity.getNextNodes();
		
		stream.writeLong(entity.getId());
		stream.writeLong(entity.getReference());
		stream.writeShort((short)page.length);
		for(int i=0;i<page.length;i++){
			stream.writeLong(page[i]);
		}
		
	}

	@Override
	protected IndexKey read(DataInputStream stream) throws IOException {
		
		IndexKey e = new IndexKey();
		e.setId(stream.readLong());
		e.setReference(stream.readLong());
		
		short pageLength = stream.readShort();
		long[] page = new long[pageLength];
		e.setNextNodes(page);
		
		for(int i=0;i<page.length;i++){
			page[i] = stream.readLong();
		}
		
		return e;
	}


	@Override
	protected void writeMetaData(DataOutputStream stream) throws IOException {
		stream.writeString("SIDX", 6);
		stream.writeShort((short)11);
		stream.writeShort((short)IndexKey.DATA_SIZE);
		stream.writeByte((byte)END_HEADER);
	}


	@Override
	protected void readMetaData(DataInputStream stream) throws IOException {
		String name = stream.readString(6);
		
		if(!"SIDX".equals(name))
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

}
