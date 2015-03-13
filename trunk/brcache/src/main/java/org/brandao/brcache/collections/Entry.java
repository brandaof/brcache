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
public class Entry<T> implements Serializable {

    private Integer index;
    
    private T item;
    
    private boolean onDisk;
    
    private NodeEntry node;
    
    private boolean needUpdate;

    public Entry(Integer index, boolean update, T item) {
        this.index = index;
        this.item = item;
        this.needUpdate = update;
    }
    
    public Entry(Integer index, T item) {
        this.index = index;
        this.item = item;
        this.needUpdate = true;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public T getItem() {
        return item;
    }

    public void setItem(T item) {
        this.item = item;
    }

    public NodeEntry getNode() {
        return node;
    }

    public void setNode(NodeEntry node) {
        this.node = node;
    }

    public boolean isOnDisk() {
        return onDisk;
    }

    public void setOnDisk(boolean onDisk) {
        this.onDisk = onDisk;
    }

    public boolean isNeedUpdate() {
        return needUpdate;
    }

    public void setNeedUpdate(boolean needUpdate) {
        this.needUpdate = needUpdate;
    }

}
