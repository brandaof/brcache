package org.brandao.brcache.collections;

import java.io.IOException;

import org.brandao.brcache.DataMap;
import org.brandao.entityfilemanager.DataReader;
import org.brandao.entityfilemanager.DataWritter;
import org.brandao.entityfilemanager.EntityFileDataHandler;

public class DataMapEntityFileDataHandler
	implements EntityFileDataHandler<DataMap, byte[], DataMapEntityFileHeader>{

	private int recordSize;
	
	private byte[] buffer;

	private byte[] empty;
	
	public DataMapEntityFileDataHandler(){
		this.recordSize = 63;
		this.buffer = new byte[this.recordSize];
		this.buffer = new byte[this.recordSize];
	}
	
	public void writeMetaData(DataWritter stream, DataMapEntityFileHeader value)
			throws IOException {
	}

	public DataMapEntityFileHeader readMetaData(DataReader srteam)
			throws IOException {
		return null;
	}

	public void writeEOF(DataWritter stream) throws IOException {
		stream.writeByte((byte)-1);
	}

	public void write(DataWritter stream, DataMap entity)
			throws IOException {
		if(entity == null){
			stream.writeByte((byte)0);
			stream.write(empty);
		}
		else{
			stream.writeByte((byte)1);
			stream.writeLong(entity.getId());
			stream.writeLong(entity.getTimeToLive());
			stream.writeLong(entity.getCreationTime());
			stream.writeLong(entity.getTimeToIdle());
			stream.writeLong(entity.getFirstSegment());
			stream.writeLong(entity.getLength());
			stream.writeLong(entity.getMostRecentTime());
			stream.writeInt(entity.getSegments());
			stream.writeShort(entity.getFlag());
		}
	}

	public void writeRaw(DataWritter stream, byte[] entity) throws IOException {
		stream.write(entity);
	}

	public DataMap read(DataReader stream) throws IOException {
		byte e = stream.readByte();
		if(e == 0){
			stream.read(buffer);
			return null;
		}
		else{
			DataMap b = new DataMap();
			b.setId(stream.readLong());
			b.setTimeToLive(stream.readLong());
			b.setCreationTime(stream.readLong());
			b.setTimeToIdle(stream.readLong());
			b.setFirstSegment(stream.readLong());
			b.setLength(stream.readLong());
			b.setMostRecentTime(stream.readLong());
			b.setSegments(stream.readInt());
			b.setFlag(stream.readShort());
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

	public Class<DataMap> getType() {
		return DataMap.class;
	}

	public Class<byte[]> getRawType() {
		return byte[].class;
	}

}
