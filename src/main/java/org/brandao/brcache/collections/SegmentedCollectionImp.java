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
class SegmentedCollectionImp<I> 
    implements SegmentedCollection<I>, Serializable{

	private static final long serialVersionUID = 239844470898102007L;

	private SwapCollection<ArraySegment<I>> swapCollection;
	
	private double fragmentSize;
	
	public SegmentedCollectionImp(
            int maxCapacity, 
            double clearFactor,
            double fragmentFactor,
            Swapper swap,
            int quantitySwaperThread){
		this.swapCollection = new SegmentedSwapCollectionImp<ArraySegment<I>>(
				maxCapacity, clearFactor, fragmentFactor, swap, quantitySwaperThread);
		
        this.fragmentSize = (int)(maxCapacity * fragmentFactor);
    }
    
    public I getEntity(long segment, int index) {

    	//o segmento (segment) é o item no swapCollection
    	Lock lock = swapCollection.getGroupLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = swapCollection.getEntry(segment);
	        
	        if (entry == null)
	            return null;
	        else{
	            return entry.getItem().get(index);
	        }
    	}
    	finally{
    		lock.unlock();
    	}
    	
    }

    public int putEntity(long segment, int index, I value) {
        
        if(this.swapCollection.isReadOnly())
            throw new IllegalStateException("readOnly");

        if(segment < 0)
            throw new IllegalStateException("segment");

        if(index < 0)
            throw new IllegalStateException("index");
        
    	//o segmento (segment) é o item no swapCollection
    	Lock lock = swapCollection.getGroupLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = swapCollection.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry == null){
	            seg = new ArraySegment<I>(segment, (int) getFragmentSize());
	            entry = new Entry<ArraySegment<I>>(segment, seg);
            	seg.set(index, value); //int idx = seg.add(value);
            	swapCollection.add(entry);
	            return index;
	        } 
	        else{
	            seg  = entry.getItem();
	            entry.setNeedUpdate(true);
	            return seg.set(index, value);
	        }
    	}
    	finally{
    		lock.unlock();
    	}
        
    }    

    public I setEntity(long segment, int index, I value) {
        
        if(swapCollection.isReadOnly())
            throw new IllegalStateException();
        
        if(segment < 0)
        	throw new IllegalStateException("segment");

        if(index < 0)
    		throw new IllegalStateException("index");
        
    	//o segmento (segment) é o item no swapCollection
    	Lock lock = swapCollection.getGroupLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = swapCollection.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry == null){
	
	            if(index != -1)
	                throw new IllegalStateException("index");
	
	            seg = new ArraySegment<I>(segment, (int) getFragmentSize());
	            entry = new Entry<ArraySegment<I>>(segment, seg);
	            seg.set(index, value);
	        	swapCollection.add(entry);
	            return null;
	        } 
	        else{
	            seg  = entry.getItem();
	            entry.setNeedUpdate(true);
	            I old = seg.get(index);
	            seg.set(index, value);
	            return old;
	        }
    	}
    	finally{
    		lock.unlock();
    	}
        
    }    

    public boolean replaceEntity(long segment, int index, I oldValue, I value) {
        
        if(swapCollection.isReadOnly())
            throw new IllegalStateException();
        
        if(segment < 0)
        	throw new IllegalStateException("segment");

        if(index < 0)
    		throw new IllegalStateException("index");
        
    	//o segmento (segment) é o item no swapCollection
    	Lock lock = swapCollection.getGroupLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = swapCollection.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry != null){
	            seg  = entry.getItem();
	            I old = seg.get(index);
	            if(old != null && old.equals(oldValue)){
	            	seg.set(index, value);
		            entry.setNeedUpdate(true);
	            	return true;
	            }
	            return false;
	        }
	        else
	        	throw new IllegalStateException("segment");
    	}
    	finally{
    		lock.unlock();
    	}
        
    }    
    
    public I replaceEntity(long segment, int index, I value) {
        
        if(swapCollection.isReadOnly())
            throw new IllegalStateException();
        
        if(segment < 0)
        	throw new IllegalStateException("segment");

        if(index < 0)
    		throw new IllegalStateException("index");
        
    	//o segmento (segment) é o item no swapCollection
    	Lock lock = swapCollection.getGroupLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = swapCollection.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry != null){
	            seg  = entry.getItem();
	            I old = seg.get(index);
	            if(old != null){
	            	seg.set(index, value);
		            entry.setNeedUpdate(true);
	            }
	            return old;
	        }
	        else
	        	throw new IllegalStateException("segment");
    	}
    	finally{
    		lock.unlock();
    	}
        
    }
    
    public I putIfAbsentEntity(long segment, int index, I value) {
        
        if(swapCollection.isReadOnly())
            throw new IllegalStateException();
        
        if(segment < 0)
        	throw new IllegalStateException("segment");

        if(index < 0)
    		throw new IllegalStateException("index");
        
    	//o segmento (segment) é o item no swapCollection
    	Lock lock = swapCollection.getGroupLock(segment);
    	lock.lock();
    	try{
	        Entry<ArraySegment<I>> entry = swapCollection.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry != null){
	            seg  = entry.getItem();
	            I old = seg.get(index);
	            if(old == null){
	            	seg.set(index, value);
		            entry.setNeedUpdate(true);
	            }
	            return old;
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

	public double getFragmentSize() {
		return fragmentSize;
	}

	public void flush() {
		swapCollection.flush();
	}

	public void setReadOnly(boolean value) {
		swapCollection.setReadOnly(value);
	}

	public boolean isReadOnly() {
		return swapCollection.isReadOnly();
	}

	public void clear() {
		swapCollection.clear();
	}

	public void destroy(){
		swapCollection.destroy();
	}
}
