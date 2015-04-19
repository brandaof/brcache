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
public class CollectionSegmentImp<I> 
    extends AbstractCollectionSegment<I,ArraySegment<I>> 
    implements Serializable{
    
    public CollectionSegmentImp(
            String pathName,
            String id, 
            int maxCapacity, 
            double clearFactor,
            double fragmentFactor,
            Swapper<ArraySegment<I>> swap,
            int quantityLock,
            int quantitySwaperThread){
        super(pathName, id, maxCapacity, 
                clearFactor, fragmentFactor, swap, quantityLock, quantitySwaperThread);
        
        int fragmentSize        = (int)(maxCapacity * fragmentFactor);
        int maxSegmentCapacity  = (int)(maxCapacity/fragmentSize);
        
        /*
        int max = (int)(maxSegmentCapacity*(1-clearFactor) + 1.0);
        
        for(int i=0;i<max;i++){
            ArraySegment<I> seg = new ArraySegment<I>(i, (int) getFragmentSize());
            Entry entry = new Entry<ArraySegment<I>>(i, seg);
            this.segments.put(i, entry);
        }
        */
        
    }
    
    @Override
    public I getEntity(Integer segment, Integer index) {
        
        Entry<ArraySegment<I>> entry = this.getEntry(segment);
        if (entry == null)
            return null;
        else{
            synchronized(this.getLock(segment)){
                entry = this.reload(entry);
                return entry.getItem().get(index);
            }
        }
        
    }

    @Override
    public int putEntity(Integer segment, Integer index, I value) {
        
        if(this.readOnly)
            throw new IllegalStateException();
        
        Entry<ArraySegment<I>> entry = super.getEntry(segment);
        ArraySegment<I> seg;

        if(entry == null){

            if(index != null)
                throw new IllegalStateException("index");

            seg = new ArraySegment<I>(segment, (int) getFragmentSize());
            entry = new Entry<ArraySegment<I>>(segment, seg);
            int idx = seg.add(value);
            addEntry(segment, entry);
            return idx;
        } 
        else{
            synchronized(this.getLock(segment)){
                entry = this.reload(entry);
                seg  = entry.getItem();
                entry.setNeedUpdate(true);
                return index != null? seg.set(index, value) : seg.add(value);
            }
        }
        
    }

    @Override
    public I removeEntity(Integer segment, Integer index){
        throw new UnsupportedOperationException();
    }

}
