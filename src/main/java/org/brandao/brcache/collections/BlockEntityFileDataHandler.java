package org.brandao.brcache.collections;

import java.io.IOException;

import org.brandao.brcache.Block;
import org.brandao.brcache.memory.Memory;
import org.brandao.brcache.memory.RegionMemory;
import org.brandao.entityfilemanager.DataReader;
import org.brandao.entityfilemanager.DataWritter;
import org.brandao.entityfilemanager.EntityFileDataHandler;

public class BlockEntityFileDataHandler 
	implements EntityFileDataHandler<Block, byte[], BlockEntityFileHeader>{

	private int blockSize;
	
	private byte[] empty;
	
	private byte[] buffer;
	
	private int recordSize;
	
	private Memory memory;
	
	public BlockEntityFileDataHandler(Memory memory, int blockSize){
		this.blockSize = blockSize;
		this.empty  = new byte[25 + blockSize];
		this.buffer = new byte[25 + blockSize];
		this.recordSize = empty.length;
		this.memory = memory;
	}
	
	public void writeMetaData(DataWritter stream, BlockEntityFileHeader value)
			throws IOException {
	}

	public BlockEntityFileHeader readMetaData(DataReader srteam)
			throws IOException {
		return new BlockEntityFileHeader();
	}

	public void writeEOF(DataWritter stream) throws IOException {
		stream.writeByte((byte)-1);
	}

	public void write(DataWritter stream, Block entity) throws IOException {
		if(entity == null){
			stream.writeByte((byte)0);
			stream.write(empty);
		}
		else{
			byte[] b = new byte[blockSize];
			entity.buffer.read(0, b, 0, (int)entity.buffer.size());
			stream.writeByte((byte)1);
			stream.writeLong(entity.id);
			stream.writeLong(entity.nextBlock);
			stream.writeInt(entity.length);
			stream.writeInt(entity.segment);
			stream.write(b);
		}
	}

	public void writeRaw(DataWritter stream, byte[] entity) throws IOException {
		stream.write(entity);
	}

	public Block read(DataReader stream) throws IOException {
		byte e = stream.readByte();
		if(e == 0){
			stream.read(buffer);
			return null;
		}
		else{
			long id = stream.readLong();
			long nextBlock = stream.readLong();
			int length = stream.readInt();
			int segment = stream.readInt();
			stream.read(buffer);
			
			RegionMemory rm = memory.alloc(blockSize);
			rm.write(0, buffer, 0, buffer.length);
			
			Block b = new Block(id, segment, rm, length);
			b.nextBlock = nextBlock;
			return b;
		}
	}

	public byte[] readRaw(DataReader stream) throws IOException {
		byte[] b = new byte[recordSize];
		stream.read(b);
		return b;
	}

	public long getFirstPointer() {
		return 0;
	}

	public int getHeaderLength() {
		return 0;
	}

	public int getRecordLength() {
		return recordSize;
	}

	public int getEOFLength() {
		return 1;
	}

	public int getFirstRecord() {
		return 0;
	}

	public Class<Block> getType() {
		return Block.class;
	}

	public Class<byte[]> getRawType() {
		return byte[].class;
	}

}