package org.brandao.brcache.collections.fileswapper;

import org.brandao.brcache.collections.Entry;

public class DiskEntry<T> extends Entry<T>{

	private long offset;

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
	
}
