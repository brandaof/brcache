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

package org.brandao.brcache.collections.treehugemap;

import java.io.Serializable;

import org.brandao.brcache.collections.ReferenceCollection;

/**
 *
 * @author Brandao
 */
public interface TreeNode<T> extends Serializable{
    
    long getId();
    
    long getValueId();
    
    void setNext(ReferenceCollection<TreeNode<T>> nodes, Object key, TreeNode<T> node);

    TreeNode<T> getNext(ReferenceCollection<TreeNode<T>> nodes, Object key);

    T setValue(ReferenceCollection<T> values, T value);

    boolean replaceValue(ReferenceCollection<T> values, T oldValue, T value);
    
    T replaceValue(ReferenceCollection<T> values, T value);
    
    T putIfAbsentValue(ReferenceCollection<T> values, T value);
    
    T removeValue(ReferenceCollection<T> values);

    boolean removeValue(ReferenceCollection<T> values, T oldValue);
    
    T getValue(ReferenceCollection<T> values);
    
}
