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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Brandao
 */
public class TreeHugeMap<K extends TreeKey,T> 
    implements Map<K,T>, Serializable{

    private static final long serialVersionUID					= 4577949145861315961L;

    public static final int DEFAULT_MAX_CAPACITY_NODE 			= 2000;
    
    public static final float DEFAULT_CLEAR_FACTOR_NODE 		= 0.25F;
    
    public static final float DEFAULT_FRAGMENT_FACTOR_NODE 		= 0.03F;
    
    public static final int DEFAULT_MAX_CAPACITY_ELEMENT 		= 1000;
    
    public static final float DEFAULT_CLEAR_FACTOR_ELEMENT 		= 0.25F;
    
    public static final float DEFAULT_FRAGMENT_FACTOR_ELEMENT   = 0.03F;
    
    private HugeArrayList<T> values;
    private HugeArrayList<Map<Object,TreeHugeMapNode<T>>> nodes;
    private TreeHugeMapNode<T> rootNode;
    
    /**
     * Cria uma nova instância usando os valores padrão.
     * 
     */
    public TreeHugeMap(){
        this(
            null, 
            DEFAULT_MAX_CAPACITY_NODE, 
            DEFAULT_CLEAR_FACTOR_NODE, 
            DEFAULT_FRAGMENT_FACTOR_NODE,
            null,
            1,
            DEFAULT_MAX_CAPACITY_ELEMENT, 
            DEFAULT_CLEAR_FACTOR_ELEMENT, 
            DEFAULT_FRAGMENT_FACTOR_ELEMENT,
            null,
            1);
    }

    public TreeHugeMap(
            String id, 
            int maxCapacityNodes,
            double clearFactorNodes, 
            double fragmentFactorNodes,
            Swapper swapNodes,
            int quantitySwaperThreadNodes,            
            int maxCapacityElements,
            double clearFactorElements, 
            double fragmentFactorElements,
            Swapper swapElements,
            int quantitySwaperThreadElements){
        
        this.values = 
            new HugeArrayList<T>(
                maxCapacityElements, 
                clearFactorElements, 
                fragmentFactorElements,
                swapElements,
                quantitySwaperThreadElements);
        
        this.nodes = 
            new HugeArrayList<Map<Object,TreeHugeMapNode<T>>>(
                maxCapacityNodes, 
                clearFactorNodes, 
                fragmentFactorNodes,
                swapNodes,
                quantitySwaperThreadNodes);

    }
    
    private boolean put(Object[] keys, int index, int limit, TreeHugeMapNode<T> node, 
            T value, boolean override){
        
        boolean needUpdate = false;
        
        if(index<limit){
            TreeHugeMapNode<T> next = node.getNextNode(this.nodes, keys[index]);
            if(next == null){
                next = new TreeHugeMapNode<T>(this.nodes);
                node.setNextNode(next, this.nodes, keys[index]);
                needUpdate = true;
            }
            
            if(put(keys, ++index, limit, next, value, override))
                node.updateNextNode(next, this.nodes, keys[index-1]);
        }
        else{
            node.setValue(value, this.values);
            needUpdate = true;
        }
        
        return needUpdate;
    }
    
    private T get(Object[] keys, int index, int limit, TreeHugeMapNode<T> node){
        
        if(index<limit){
            TreeHugeMapNode<T> next = node.getNextNode(this.nodes, keys[index]);
            if(next == null)
                return null;
            else
                return (T) get(keys, ++index, limit, next);
        }
        else
            return node.getValue(this.values);
        
    }

    private T remove(Object[] keys, int index, int limit, TreeHugeMapNode<T> node){
        
        if(index<limit){
            TreeHugeMapNode<T> next = node.getNextNode(this.nodes, keys[index]);
            if(next == null)
                return null;
            else
                return (T) get(keys, ++index, limit, next);
        }
        else{
            T value = node.getValue(this.values);
            node.removeValue(this.values);
            return value;
        }
        
    }
    
    public synchronized T put(K key, T element){
        
        Object[] keys = key.getNodes();
        
        if(this.rootNode == null)
            this.rootNode = new TreeHugeMapNode<T>(nodes);
        
        put(keys, 0, keys.length, this.rootNode, element, false);
        
        return null;
    }
    
    public int size() {
        return this.values.size();
    }

    public boolean isEmpty() {
        return this.values.isEmpty();
    }

    public boolean containsKey(Object key) {
        return this.get(key) != null;
    }

    public boolean containsValue(Object value) {
        return this.values.contains(value);
    }

    public T get(Object key) {
        Object[] keys = ((TreeKey)key).getNodes();
        if(this.rootNode == null)
            return null;
        else
            return this.get(keys, 0, keys.length, this.rootNode);
    }

    public synchronized T remove(Object key) {
        Object[] keys = ((TreeKey)key).getNodes();
        if(this.rootNode != null)
            return this.remove(keys, 0, keys.length, this.rootNode);
        else
            return null;
    }

    public void putAll(Map<? extends K, ? extends T> m) {
        for(K key: m.keySet())
            this.put(key, m.get(key));
    }

    public void clear() {
        this.values.clear();
        this.nodes.clear();
        this.rootNode = null;
    }

    public void flush(){
        this.nodes.flush();
        this.values.flush();
    }
    
    public Set<K> keySet() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    public Collection<T> values() {
        return this.values;
    }

    public Set<Entry<K, T>> entrySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setReadOnly(boolean value){
        this.nodes.setReadOnly(value);
        this.values.setReadOnly(value);
    }
    
    public boolean isReadOnly(){
        return this.values.isReadOnly();
    }
    
}
