package org.brandao.brcache.tx;

public class EntryCache {

	private byte[] data;
	
	private long maxAlive;

	public EntryCache(byte[] data, long maxAlive) {
		this.data = data;
		this.maxAlive = maxAlive;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public long getMaxAlive() {
		return maxAlive;
	}

	public void setMaxAlive(long maxAlive) {
		this.maxAlive = maxAlive;
	}
	
}
