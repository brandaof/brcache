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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Brandao
 */
public class TreeHugeMapNode<T> implements Serializable{
    
	private static final long serialVersionUID = -5212207702622818424L;

	private final Integer nodeIndex;
    
    private volatile int valueIndex;
    
    public TreeHugeMapNode(List<Map<Object,TreeHugeMapNode<T>>> nodes){
    	
        this.valueIndex = -1;
        Map<Object,TreeHugeMapNode<T>> node = new HashMap<Object,TreeHugeMapNode<T>>();
        
        synchronized(nodes){
	        nodes.add(node);
	        this.nodeIndex = nodes.size() - 1;
        }
        
    }
    
    public TreeHugeMapNode<T> getNextNode(List<Map<Object,TreeHugeMapNode<T>>> nodes, 
            Object key){
    	
        Map<Object,TreeHugeMapNode<T>> thisNode = nodes.get(nodeIndex);
        return thisNode.get(key);
        
    }

    public void setNextNode(TreeHugeMapNode<T> next, 
            List<Map<Object,TreeHugeMapNode<T>>> nodes, Object key){
    	
        synchronized(nodes){
            Map<Object,TreeHugeMapNode<T>> thisNode = nodes.get(nodeIndex);
            thisNode.put(key, next);
            nodes.set(nodeIndex, thisNode);
        }
        
    }

    public void updateNextNode(TreeHugeMapNode<T> next, 
            List<Map<Object,TreeHugeMapNode<T>>> nodes, Object key){
        this.setNextNode(next, nodes, key);
    }
    
    public void setValue(T value, List<T> values){
        
        synchronized(values){
            if( this.valueIndex == -1){
                values.add(value);
                this.valueIndex = values.size() - 1;
            }
            else
                values.set(this.valueIndex, value);
        }
    }
    
    public T getValue(List<T> values){
    	
        if(this.valueIndex == -1)
            return null;
        else
            return values.get(this.valueIndex);
    }

    public void overrideValue(T value, List<T> values){
    	
        synchronized(values){
            if(this.valueIndex == -1)
                throw new IllegalArgumentException();

            values.set(this.valueIndex, value);
        }
    }
    
    public void removeValue(List<T> values){
    	
        synchronized(values){
            if(this.valueIndex == -1)
                throw new IllegalArgumentException();

            values.remove(this.valueIndex);
        }
    }
}
