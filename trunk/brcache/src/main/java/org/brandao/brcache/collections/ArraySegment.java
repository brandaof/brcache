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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * 
 * @author Brandao.
 * 
 */
@SuppressWarnings("serial")
class ArraySegment<K> implements Serializable {

    private int id;
    private int size;
    private Object[] data;
    private int segmentSize;

    public ArraySegment(int id, int segmentSize) {
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

    public int add(K value) {
        if (data == null) {
            data = new Object[segmentSize];
        }

        if (size >= data.length)
            data = Arrays.copyOf(data, data.length + 10);
        
        int index = size;
        data[index] = value;
        size++;
        return index;
    }

    public int set(int index, K value) {
        if (data == null) {
            data = new Object[segmentSize];
        }

        if (index >size)
            throw new IndexOutOfBoundsException(index + " > " + size);
        
        data[index] = value;
        return index;
    }
    
    @SuppressWarnings("unchecked")
    public K remove(int index) {

        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }

        K oldValue = (K) data[index];
        int moved = size - index - 1;

        if (moved > 0) {
            System.arraycopy(data, index + 1, data, index, moved);
        }

        data[--size] = null;

        return oldValue;
    }

    @SuppressWarnings("unchecked")
    public K get(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException(index + " <> " + size);
        }

        return (K) data[index];
    }

    public Object[] ToArray() {
        return data;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(id);
        out.writeInt(size);
        out.writeObject(data);
        out.writeInt(segmentSize);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        id = in.readInt();
        size = in.readInt();
        data = (Object[]) in.readObject();
        segmentSize = in.readInt();
    }
}
