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

/**
 *
 * @author Brandao
 */
class CollectionSegmentImp<I> 
    extends AbstractCollectionSegment<I,ArraySegment<I>> 
    implements Serializable{

	private static final long serialVersionUID = 239844470898102007L;

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
    }
    
    public I getEntity(long segment, int index) {
        
    	Object lock = this.getLock(segment);
    	synchronized(lock){
	        Entry<ArraySegment<I>> entry = this.getEntry(segment);
	        
	        if (entry == null)
	            return null;
	        else{
	            //entry = this.reload(entry);
	            return entry.getItem().get(index);
	        }
    	}
    }

    public int putEntity(long segment, int index, I value) {
        
        if(this.readOnly)
            throw new IllegalStateException();
        
    	Object lock = this.getLock(segment);
    	
        synchronized(lock){
        	
	        Entry<ArraySegment<I>> entry = super.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry == null){
	
	            if(index != -1)
	                throw new IllegalStateException("index");
	
	            seg = new ArraySegment<I>(segment, (int) getFragmentSize());
	            entry = new Entry<ArraySegment<I>>(segment, seg);
	            int idx = seg.add(value);
	        	addEntry(segment, entry);
	            return idx;
	        } 
	        else{
                seg  = entry.getItem();
                entry.setNeedUpdate(true);
                return index != -1? seg.set(index, value) : seg.add(value);
	        }
        }
        
    }    
    /*
    public int putEntity(long segment, int index, I value) {
        
        if(this.readOnly)
            throw new IllegalStateException();
        
    	Object lock = this.getLock(segment);
    	
        synchronized(lock){
        	
	        Entry<ArraySegment<I>> entry = super.getEntry(segment);
	        ArraySegment<I> seg;
		
	        if(entry == null){
	
	            if(index != -1)
	                throw new IllegalStateException("index");
	
	            seg = new ArraySegment<I>(segment, (int) getFragmentSize());
	            entry = new Entry<ArraySegment<I>>(segment, seg);
	            int idx = seg.add(value);
	        	addEntry(segment, entry);
	            return idx;
	        } 
	        else{
                entry = this.reload(entry);
                seg  = entry.getItem();
                entry.setNeedUpdate(true);
                return index != -1? seg.set(index, value) : seg.add(value);
	        }
        }
        
    }
    */
    public I removeEntity(long segment, int index){
        throw new UnsupportedOperationException();
    }

}