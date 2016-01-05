package org.brandao.brcache.collections.fileswapper;

public class DataBlock {

	private long id;
	
	private byte[] data;
	
	private long nextBlock;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public long getNextBlock() {
		return nextBlock;
	}

	public void setNextBlock(long nextBlock) {
		this.nextBlock = nextBlock;
	}
	
}
