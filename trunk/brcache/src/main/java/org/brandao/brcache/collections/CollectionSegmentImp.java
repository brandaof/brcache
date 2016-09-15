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

package org.brandao.brcache.collections;

import java.io.Serializable;
import java.util.concurrent.locks.Lock;

/**
 *
 * @author Brandao
 */
class CollectionSegmentImp<I> 
    extends AbstractCollectionSegment<I,ArraySegment<I>> 
    implements Serializable{

	private static final long serialVersionUID = 239844470898102007L;

	private RouletteLock locks;

	public CollectionSegmentImp(
            String id, 
            int maxCapacity, 
            double clearFactor,
            double fragmentFactor,
            Swapper swap,
            int quantitySwaperThread){
        super(id, maxCapacity, 
                clearFactor, fragmentFactor, swap, 
                quantitySwaperThread);
        
        this.locks = new RouletteLock(1000);
    }
    
    public I getEntity(long segment, int index) {
        
    	Lock lock = this.locks.getLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = this.getEntry(segment);
	        
	        if (entry == null)
	            return null;
	        else{
	        	synchronized(super.getLock(segment)){
		            entry = this.reload(entry);
		            return entry.getItem().get(index);
	        	}
	        }
    	}
    	finally{
    		lock.unlock();
    	}
    	
    }

    public int putEntity(long segment, int index, I value) {
        
        if(this.readOnly)
            throw new IllegalStateException("readOnly");

        if(segment < 0)
            throw new IllegalStateException("segment");

        if(index < 0)
            throw new IllegalStateException("index");
        
    	Lock lock = this.locks.getLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = super.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry == null){
	            seg = new ArraySegment<I>(segment, (int) getFragmentSize());
	            entry = new Entry<ArraySegment<I>>(segment, seg);
            	seg.set(index, value); //int idx = seg.add(value);
            	addEntry(segment, entry);
	            return index;
	        } 
	        else{
	        	synchronized(super.getLock(segment)){
	        		entry = super.reload(entry);
		            seg  = entry.getItem();
		            entry.setNeedUpdate(true);
		            return seg.set(index, value);
	        	}
	        }
    	}
    	finally{
    		lock.unlock();
    	}
        
    }    

    public I setEntity(long segment, int index, I value) {
        
        if(this.readOnly)
            throw new IllegalStateException();
        
        if(segment < 0)
        	throw new IllegalStateException("segment");

        if(index < 0)
    		throw new IllegalStateException("index");
        
    	Lock lock = this.locks.getLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = super.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry == null){
	
	            if(index != -1)
	                throw new IllegalStateException("index");
	
	            seg = new ArraySegment<I>(segment, (int) getFragmentSize());
	            entry = new Entry<ArraySegment<I>>(segment, seg);
	            seg.set(index, value);
	        	addEntry(segment, entry);
	            return null;
	        } 
	        else{
	        	synchronized(super.getLock(segment)){
	        		entry = super.reload(entry);
		            seg  = entry.getItem();
		            entry.setNeedUpdate(true);
		            I old = seg.get(index);
		            seg.set(index, value);
		            return old;
	        	}
	        }
    	}
    	finally{
    		lock.unlock();
    	}
        
    }    

    public boolean replaceEntity(long segment, int index, I oldValue, I value) {
        
        if(this.readOnly)
            throw new IllegalStateException();
        
        if(segment < 0)
        	throw new IllegalStateException("segment");

        if(index < 0)
    		throw new IllegalStateException("index");
        
    	Lock lock = this.locks.getLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = super.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry != null){
	        	synchronized(super.getLock(segment)){
	        		entry = super.reload(entry);
		            seg  = entry.getItem();
		            I old = seg.get(index);
		            if(old != null && old.equals(value)){
		            	seg.set(index, value);
			            entry.setNeedUpdate(true);
		            	return true;
		            }
		            return false;
	        	}
	        }
	        else
	        	throw new IllegalStateException("segment");
    	}
    	finally{
    		lock.unlock();
    	}
        
    }    
    
    public I replaceEntity(long segment, int index, I value) {
        
        if(this.readOnly)
            throw new IllegalStateException();
        
        if(segment < 0)
        	throw new IllegalStateException("segment");

        if(index < 0)
    		throw new IllegalStateException("index");
        
    	Lock lock = this.locks.getLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = super.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry != null){
	        	synchronized(super.getLock(segment)){
	        		entry = super.reload(entry);
		        	
		            seg  = entry.getItem();
		            I old = seg.get(index);
		            if(old != null){
		            	seg.set(index, value);
			            entry.setNeedUpdate(true);
		            }
		            return old;
	        	}
	        }
	        else
	        	throw new IllegalStateException("segment");
    	}
    	finally{
    		lock.unlock();
    	}
        
    }
    
    public I putIfAbsentEntity(long segment, int index, I value) {
        
        if(this.readOnly)
            throw new IllegalStateException();
        
        if(segment < 0)
        	throw new IllegalStateException("segment");

        if(index < 0)
    		throw new IllegalStateException("index");
        
    	Lock lock = this.locks.getLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = super.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry != null){
	        	synchronized(super.getLock(segment)){
	        		entry = super.reload(entry);
		            seg  = entry.getItem();
		            I old = seg.get(index);
		            if(old == null){
		            	seg.set(index, value);
			            entry.setNeedUpdate(true);
		            }
		            return old;
	        	}
	        }
	        else
	        	throw new IllegalStateException("segment");
    	}
    	finally{
    		lock.unlock();
    	}
        
    }     
    public I removeEntity(long segment, int index){
        throw new UnsupportedOperationException();
    }

}
