package org.brandao.brcache.collections.fileswapper;

public class DataBlockGroup {

	private long[] segments;
	
	private int len;
	
	private long nextGroup;

	public long[] getSegments() {
		return segments;
	}

	public void setSegments(long[] segments) {
		this.segments = segments;
	}

	public int getLen() {
		return len;
	}

	public void setLen(int len) {
		this.len = len;
	}

	public long getNextGroup() {
		return nextGroup;
	}

	public void setNextGroup(long nextGroup) {
		this.nextGroup = nextGroup;
	}
	
}
