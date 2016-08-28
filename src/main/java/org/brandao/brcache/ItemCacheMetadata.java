package org.brandao.brcache;

import java.io.Serializable;


public class ItemCacheMetadata implements Serializable{

	private static final long serialVersionUID = 1059371320140414547L;

	private long id;
	
	private long timeToLive; 
	
	private long timeToIdle;
	
	private long creationTime;
	
	private long mostRecentTime; 
	
	private long timeToLiveRemaining;
	
	private long timeToIdleRemaining;
	
	private short flag; 
	
	private long size;	
	
	public ItemCacheMetadata(CacheInputStream item){
		this.id                     = item.getId();
		this.timeToIdleRemaining	= item.getTimeToIdleRemaining();
		this.timeToLiveRemaining	= item.getTimeToLiveRemaining();
		this.creationTime 			= item.getCreationTime();
		this.flag 					= item.getFlag();
		this.mostRecentTime			= item.getMostRecentTime();
		this.size 					= item.getSize();
		this.timeToIdle				= item.getTimeToIdle();
		this.timeToLive				= item.getTimeToLive();
		
	}

	public ItemCacheMetadata(long id, long timeToLive, long timeToIdle,
			long creationTime, long mostRecentTime, long timeToLiveRemaining,
			long timeToIdleRemaining, short flag, long size) {
		this.id = id;
		this.timeToLive = timeToLive;
		this.timeToIdle = timeToIdle;
		this.creationTime = creationTime;
		this.mostRecentTime = mostRecentTime;
		this.timeToLiveRemaining = timeToLiveRemaining;
		this.timeToIdleRemaining = timeToIdleRemaining;
		this.flag = flag;
		this.size = size;
	}

	public long getId() {
		return id;
	}

	public long getTimeToLive() {
		return timeToLive;
	}

	public long getTimeToIdle() {
		return timeToIdle;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public long getMostRecentTime() {
		return mostRecentTime;
	}

	public short getFlag() {
		return flag;
	}

	public long getSize() {
		return size;
	}

	public long getTimeToLiveRemaining() {
		return timeToLiveRemaining;
	}

	public long getTimeToIdleRemaining() {
		return timeToIdleRemaining;
	}
	
}
