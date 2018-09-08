package org.brandao.brcache.collections;

import java.io.IOException;

import org.brandao.brcache.collections.treehugemap.CharNode;
import org.brandao.entityfilemanager.DataReader;
import org.brandao.entityfilemanager.DataWritter;
import org.brandao.entityfilemanager.EntityFileDataHandler;

@SuppressWarnings("rawtypes")
public class CharNodeEntityFileDataHandler
	implements EntityFileDataHandler<CharNode, byte[], CharNodeEntityFileHeader>{

	private int recordSize;
	
	private byte[] buffer;

	private byte[] empty;
	
	public CharNodeEntityFileDataHandler(){
		this.recordSize = CharNode.DATA_SIZE + 17;
		this.buffer = new byte[this.recordSize];
		this.empty  = new byte[this.recordSize];
	}
	
	public void writeMetaData(DataWritter stream, CharNodeEntityFileHeader value)
			throws IOException {
	}

	public CharNodeEntityFileHeader readMetaData(DataReader srteam)
			throws IOException {
		return null;
	}

	public void writeEOF(DataWritter stream) throws IOException {
		stream.writeByte((byte)-1);
	}

	public void write(DataWritter stream, CharNode entity)
			throws IOException {
		if(entity == null){
			stream.writeByte((byte)0);
			stream.write(empty);
		}
		else{
			stream.writeByte((byte)1);
			stream.writeLong(entity.getId());
			stream.writeLong(entity.getValueId());
			
			for(long n: entity.getNextNodes()){
				stream.writeLong(n);
			}
		}
	}

	public void writeRaw(DataWritter stream, byte[] entity) throws IOException {
		stream.write(entity);
	}

	public CharNode read(DataReader stream) throws IOException {
		byte e = stream.readByte();
		if(e == 0){
			stream.read(buffer);
			return null;
		}
		else{
			long id = stream.readLong();
			long valueId = stream.readLong();
			long[] nextNodes = new long[CharNode.LEN_NODES];
			
			for(int i=0;i<nextNodes.length;i++){
				nextNodes[i] = stream.readLong();
			}
			
			CharNode b = new CharNode(id, valueId, nextNodes);
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

	public Class<CharNode> getType() {
		return CharNode.class;
	}

	public Class<byte[]> getRawType() {
		return byte[].class;
	}

}
