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
public class GroupCharTreeKey implements TreeKey{

    private String val;
    
    private Object[] key;
    
    public GroupCharTreeKey(String value){
        int length = value.length();
        int desloc = 2;
        int count = length/2;
        count += length % 2 != 0? 1 : 0;
        
        this.key = new Object[count];
        
        int start = 0;
        for(int i=0;i<count-1;i++){
            this.key[i] = value.substring(start, start + desloc);
            start += desloc;
        }
        
        if(start < length)
            this.key[count - 1] = value.substring(start, start + (length - start));
        
        this.val = value;
            
    }
    
    public Object[] getNodes() {
        return this.key;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + (this.val != null ? this.val.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GroupCharTreeKey other = (GroupCharTreeKey) obj;
        if ((this.val == null) ? (other.val != null) : !this.val.equals(other.val)) {
            return false;
        }
        return true;
    }
    
}
