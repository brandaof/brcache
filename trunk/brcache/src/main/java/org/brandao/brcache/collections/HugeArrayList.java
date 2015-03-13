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
import java.util.*;

/**
 *
 * @author Brandao
 */
public class HugeArrayList<T> implements HugeList<T>,Serializable{

    public static final int DEFAULT_MAX_CAPACITY_ELEMENT = 1000;
    
    public static final float DEFAULT_CLEAR_FACTOR_ELEMENT = 0.25F;
    
    public static final float DEFAULT_FRAGMENT_FACTOR_ELEMENT = 0.03F;

    private int size;
    
    private CollectionSegmentImp<T> elements;
    
    public HugeArrayList() {
        this(
            null,
            Collections.getNextId(), 
            DEFAULT_MAX_CAPACITY_ELEMENT, 
            DEFAULT_CLEAR_FACTOR_ELEMENT, 
            DEFAULT_FRAGMENT_FACTOR_ELEMENT);
    }
    
    public HugeArrayList(
            String pathName,
            String id, 
            int maxCapacityElements,
            double clearFactorElements, 
            double fragmentFactorElements) {
        
        this.size = 0;
        id = id == null? Collections.getNextId() : id;
        this.elements = 
                new CollectionSegmentImp<T>(
                pathName,
                id,
                maxCapacityElements, 
                clearFactorElements, 
                fragmentFactorElements);
    }
    
    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean contains(Object o) {
        for(int i=0;i<size;i++){
            T item = get(i);
            if(item != null && item.equals(o))
                    return true;
        }
        return false;
    }

    public Iterator<T> iterator() {
        return new HugeIterator<T>(this);
    }

    public Object[] toArray() {
        return toArray(new Object[]{});
    }

    public <T> T[] toArray(T[] a) {
        T[] result = Arrays.copyOf(a, size);

        for(int i=0;i<size;i++)
            result[i] = (T)get(i);

        return result;
    }

    public T get(int index) {
        if(index>size)
            throw new IndexOutOfBoundsException(index + " > " + size);
        
        int segmentId = (int)(index/this.elements.getFragmentSize());
        int idx     = (int)(index%this.elements.getFragmentSize());
        return this.elements.getEntity(segmentId, idx);
    }
    
    public boolean add(T e) {
        int group = (int) (size / elements.getFragmentSize());
        elements.putEntity(group, null, e);
        size++;
        return true;
    }

    public T set(int index, T element) {
        if(index>size)
            throw new IndexOutOfBoundsException(index + " > " + size);
        
        int segmentId = (int)(index/this.elements.getFragmentSize());
        int idx     = (int)(index%this.elements.getFragmentSize());
        T old = this.elements.getEntity(segmentId, idx);
        this.elements.putEntity(segmentId, idx, element);
        return old;
    }
    
    public synchronized T remove(int index) {
        T old = this.get(index);
        
        for(int i=index; i<size;i++){
            T next = i + 1 < size? this.get(i + 1) : null;
            this.set(index, next);
        }
        
        size--;
        return old;
    }
    
    public boolean remove(Object o) {
        
        int index = -1;
        
        for(int i=0;i<size;i++){
            if(o.equals(get(i))){
                index = i;
                break;
            }
        }
        
        if(index != -1)
            return this.remove(index) != null;
        else
            return false;
    }

    public boolean containsAll(Collection<?> c) {
        for(Object entity: c)
            if(!this.contains(entity))
                return false;
        return true;
    }

    public boolean addAll(Collection<? extends T> c) {
        for (T type : c)
            this.add(type);
        return true;
    }

    public boolean addAll(int index, Collection<? extends T> c) {
        int i = 0;
        for (T type : c){
            if(i++>=index)
                this.add(type);
        }
        return true;
    }

    public boolean removeAll(Collection<?> c) {
        for (Object entity : c)
            this.remove(entity);
        return true;
    }

    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void clear() {
        this.elements.clear();
        this.size = 0;
    }

    public void add(int index, T element) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int indexOf(Object o) {
        for(int i=0;i<size;i++)
            if(o.equals((T)get(i)))
                return i;
        return -1;
    }

    public void flush(){
        this.elements.flush();
    }
    
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setReadOnly(boolean value) {
        this.elements.setReadOnly(value);
    }

    public boolean isReadOnly() {
        return this.elements.isReadOnly();
    }
    
}
