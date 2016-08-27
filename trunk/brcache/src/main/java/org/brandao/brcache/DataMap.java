/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache;

import java.io.Serializable;

/**
 * 
 * @author Brandao
 */
class DataMap implements Serializable{
    
	private static final long serialVersionUID = -2531845084336308095L;

	private long id;
    
    private long timeToLive;
    
    private long creationTime;
    
    private long timeToIdle;
    
    private short flag;
    
    private int firstSegment;
    
    private int segments;
    
    private long length;
    
    private long mostRecentTime;
    
    public long getExpirationTime(){
    	long ttlExpiry = timeToLive == 0? Long.MAX_VALUE : creationTime + timeToLive;
		long ttiExpiry = timeToIdle == 0? Long.MAX_VALUE : mostRecentTime + timeToIdle;	
    	return Math.min(ttlExpiry, ttiExpiry);
    }

    public long getTimeToLiveRemaining(){
    	if(this.timeToLive == 0){
    		return 0;
    	}
    	else{
	    	long currentTime         = System.currentTimeMillis();
	    	long timeToLiveRemaining = creationTime + timeToLive - currentTime;
	    	return timeToLiveRemaining;
    	}
    }
    
    public boolean isDead(){
    	long currentTime = System.currentTimeMillis();
    	return currentTime > this.getExpirationTime();
    }
    
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getTimeToLive() {
		return timeToLive;
	}

	public void setTimeToLive(long timeToLive) {
		this.timeToLive = timeToLive;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	public long getTimeToIdle() {
		return timeToIdle;
	}

	public void setTimeToIdle(long timeToIdle) {
		this.timeToIdle = timeToIdle;
	}

	public short getFlag() {
		return flag;
	}

	public void setFlag(short flag) {
		this.flag = flag;
	}

	public int getFirstSegment() {
		return firstSegment;
	}

	public void setFirstSegment(int firstSegment) {
		this.firstSegment = firstSegment;
	}

	public int getSegments() {
		return segments;
	}

	public void setSegments(int segments) {
		this.segments = segments;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public long getMostRecentTime() {
		return mostRecentTime;
	}

	public void setMostRecentTime(long mostRecentTime) {
		this.mostRecentTime = mostRecentTime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataMap other = (DataMap) obj;
		if (id != other.id)
			return false;
		return true;
	}

}
