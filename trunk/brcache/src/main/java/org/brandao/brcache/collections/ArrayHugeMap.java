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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Brandao
 */
public class ArrayHugeMap<K,T> 
    implements HugeMap<K,T>{

    public int size() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public T get(Object key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public T put(K key, T value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public T remove(Object key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void putAll(Map<? extends K, ? extends T> m) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void clear() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Set<K> keySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Collection<T> values() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Set<Entry<K, T>> entrySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
