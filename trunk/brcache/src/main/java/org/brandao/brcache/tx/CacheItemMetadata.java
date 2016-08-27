package org.brandao.brcache.tx;

import java.io.Serializable;

public class CacheItemMetadata implements Serializable{

	private static final long serialVersionUID = -4493838322220927274L;

	private long timeToLive;
	
	private long timeToIdle;

	public CacheItemMetadata(long timeToLive, long timeToIdle) {
		this.timeToLive = timeToLive;
		this.timeToIdle = timeToIdle;
	}

	public long getTimeToLive() {
		return timeToLive;
	}

	public void setTimeToLive(long timeToLive) {
		this.timeToLive = timeToLive;
	}

	public long getTimeToIdle() {
		return timeToIdle;
	}

	public void setTimeToIdle(long timeToIdle) {
		this.timeToIdle = timeToIdle;
	}
	
}
