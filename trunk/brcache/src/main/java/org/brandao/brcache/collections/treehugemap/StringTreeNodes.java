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

import java.util.List;

/**
 *
 * @author Brandao
 */
public class StringTreeNodes<T> implements TreeNodes<T>{

	private static final long serialVersionUID = -8387188156629418047L;

	public TreeMapKey getKey(Object key) {
    	String strKey = (String)key;
    	strKey = strKey.toLowerCase();
        StringTreeMapKey k = new StringTreeMapKey();
        k.index = strKey.toCharArray();
        k.limit = k.index.length;
        k.pos   = 0;
        return k;
    }

    public boolean isEquals(TreeMapKey key, TreeNode<T> node) {
        StringTreeMapKey k = (StringTreeMapKey)key;
        return k.pos == k.limit;
    }

    public T getValue(List<T> values, TreeNode<T> node){
    	synchronized(values){
    		return node.getValue(values);
    	}
    }
    
    public T setValue(List<T> values, TreeNode<T> node, T value){
    	synchronized(values){
    		T old = node.getValue(values);
    		node.setValue(values, value);
    		return old;
    	}
    }
    
    public T removeValue(List<T> values, TreeNode<T> node) {
    	synchronized(values){
    		T old = node.getValue(values);
    		node.removeValue(values);
    		return old;
    	}
    }
    
    public TreeNode<T> getNext(List<TreeNode<T>> nodes, TreeMapKey key, TreeNode<T> node, boolean read) {
        StringTreeMapKey k = (StringTreeMapKey)key;
        
        char i = k.index[k.pos++];
        TreeNode<T> next = node.getNext(nodes, i);
        
        if(next == null && !read){
            synchronized(nodes){
                node = nodes.get((int)node.getId());
                
                next = node.getNext(nodes, i);
                
                if(next != null)
                    return next;
                
                CharNode<T> nextNode = new CharNode<T>();
                nodes.add(nextNode);
                nextNode.setId(nodes.size() - 1);
                next = nextNode;
                
                node.setNext(nodes, i, nextNode);
            }
        }
        
        return next;
    }

    public TreeNode<T> getFirst(List<TreeNode<T>> nodes) {
        return nodes.isEmpty()? null : nodes.get(0);
    }

    public void init(List<TreeNode<T>> nodes) {
        if(!nodes.isEmpty())
            throw new IllegalStateException();
        
        CharNode<T> node = new CharNode<T>();
        node.setId(0);
        nodes.add(node);
    }
    
    private static class StringTreeMapKey implements TreeMapKey{
        
        public char[] index;
        
        public int pos;
        
        public int limit;
        
    }
}
