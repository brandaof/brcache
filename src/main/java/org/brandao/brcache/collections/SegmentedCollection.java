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

/**
 *
 * @author Brandao
 */
interface SegmentedCollection<T> {
    
    T getEntity(long segment, int index);

    int putEntity(long segment, int index, T value);

    T removeEntity(long segment, int index);
    
    T setEntity(long segment, int index, T value);
    
    boolean replaceEntity(long segment, int index, T oldValue, T value);
    
    T replaceEntity(long segment, int index, T value);
    
    T putIfAbsentEntity(long segment, int index, T value);
    
    double getFragmentSize();
    
    void flush();
    
    void clear();
    
    void destroy();
    
    void setReadOnly(boolean value);
    
    boolean isReadOnly();
    
}
