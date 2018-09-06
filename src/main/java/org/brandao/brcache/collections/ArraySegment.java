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
 * @author Brandao.
 * 
 */
class ArraySegment<K> 
	implements Serializable {

	private static final long serialVersionUID = -6110258049047837285L;

	private long id;
    
    private int size;
    
    private Object[] data;
    
    private int segmentSize;

    public ArraySegment(long id, int segmentSize) {
        this.segmentSize = segmentSize;
        this.id = id;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Object[] getData() {
        return data;
    }

    public void setData(K[] data) {
        this.data = data;
    }

    public int set(int index, K value) {
        if (data == null) {
            data = new Object[segmentSize];
        }

        if (index >= data.length)
            throw new IndexOutOfBoundsException(index + " >= " + size);
        
        data[index] = value;
        return index;
    }
    
    @SuppressWarnings("unchecked")
    public K remove(int index) {

        if (index < 0 || index >= this.data.length) {
            throw new IndexOutOfBoundsException();
        }

        K oldValue = (K) data[index];
        data[index] = null;
        return oldValue;
    }

    @SuppressWarnings("unchecked")
    public K get(int index) {
        if (index >= this.data.length) {
            throw new IndexOutOfBoundsException(index + " >= " + size);
        }

        return (K) data[index];
    }

    public Object[] ToArray() {
        return data;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
    
}
