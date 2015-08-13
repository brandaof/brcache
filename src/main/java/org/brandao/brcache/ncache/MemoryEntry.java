package org.brandao.brcache.ncache;

public class MemoryEntry {

	private long size;
	
	private Integer[] segments;

	public MemoryEntry(long size, Integer[] segments) {
		this.size = size;
		this.segments = segments;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public Integer[] getSegments() {
		return segments;
	}

	public void setSegments(Integer[] segments) {
		this.segments = segments;
	}
	
	
}
