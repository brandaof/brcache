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

import java.util.concurrent.locks.Lock;

import org.brandao.brcache.collections.ReferenceCollection;
import org.brandao.brcache.collections.RouletteLock;

/**
 *
 * @author Brandao
 */
public class StringTreeNodes<T> implements TreeNodes<T>{

	private static final long serialVersionUID = -8387188156629418047L;

	private RouletteLock locks;
	
	private long firstNodeId;
	
	public StringTreeNodes(){
		this.locks       = new RouletteLock();
		this.firstNodeId = -1;
	}
	
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

    public T getValue(ReferenceCollection<T> values, TreeNode<T> node){
		return node.getValue(values);
    }
    
    public T setValue(ReferenceCollection<T> values, TreeNode<T> node, T value){
    	Lock lock = this.locks.getLock(node.getId());
    	lock.lock();
    	try{
    		return node.setValue(values, value);
    	}
    	finally{
    		lock.unlock();
    	}
    }
    
    public boolean replaceValue(ReferenceCollection<T> values, TreeNode<T> node, T oldValue, T value){
    	Lock lock = this.locks.getLock(node.getId());
    	lock.lock();
    	try{
    		return node.replaceValue(values, oldValue, value);
    	}
    	finally{
    		lock.unlock();
    	}
    }

    public T replaceValue(ReferenceCollection<T> values, TreeNode<T> node, T value){
    	Lock lock = this.locks.getLock(node.getId());
    	lock.lock();
    	try{
    		return node.replaceValue(values, value);
    	}
    	finally{
    		lock.unlock();
    	}
    }

    public T putIfAbsentValue(ReferenceCollection<T> values, TreeNode<T> node, T value){
    	Lock lock = this.locks.getLock(node.getId());
    	lock.lock();
    	try{
    		return node.putIfAbsentValue(values, value);
    	}
    	finally{
    		lock.unlock();
    	}
    }
    
    public T removeValue(ReferenceCollection<T> values, TreeNode<T> node) {
    	Lock lock = this.locks.getLock(node.getId());
    	lock.lock();
    	try{
			return node.removeValue(values);
    	}
    	finally{
    		lock.unlock();
    	}
    }

    public boolean removeValue(ReferenceCollection<T> values, TreeNode<T> node, T oldValue) {
    	Lock lock = this.locks.getLock(node.getId());
    	lock.lock();
    	try{
    		return node.removeValue(values, oldValue);
    	}
    	finally{
    		lock.unlock();
    	}
    }
    
    public TreeNode<T> getNext(ReferenceCollection<TreeNode<T>> nodes, TreeMapKey key, TreeNode<T> node, boolean read) {
        StringTreeMapKey k = (StringTreeMapKey)key;
        
        char i = k.index[k.pos++];
        TreeNode<T> next = node.getNext(nodes, i);
        
        if(next == null && !read){
        	Lock lock = this.locks.getLock(node.getId());
        	lock.lock();
        	try{
                node = nodes.get(node.getId());
                next = node.getNext(nodes, i);
                
                if(next != null)
                    return next;
                
                CharNode<T> nextNode = new CharNode<T>();
                
                long id = nodes.insert(nextNode);
                
                nextNode.setId(id);
                next = nextNode;
                
                node.setNext(nodes, i, nextNode);
        	}
        	finally{
        		lock.unlock();
        	}
        	
        }
        
        return next;
    }

    public TreeNode<T> getFirst(ReferenceCollection<TreeNode<T>> nodes) {
        return nodes.isEmpty()? null : nodes.get(this.firstNodeId);
    }

    public void init(ReferenceCollection<TreeNode<T>> nodes) {
        if(!nodes.isEmpty())
            throw new IllegalStateException();
        
        CharNode<T> node = new CharNode<T>();
        node.setId(nodes.insert(node));
        nodes.set(node.getId(), node);
        this.firstNodeId = node.getId();
    }
    
    private static class StringTreeMapKey implements TreeMapKey{
        
        public char[] index;
        
        public int pos;
        
        public int limit;
        
    }
}
