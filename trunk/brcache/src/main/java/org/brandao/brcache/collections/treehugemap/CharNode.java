package org.brandao.brcache.collections.treehugemap;

import java.io.Serializable;

import org.brandao.brcache.collections.ReferenceCollection;

public class CharNode<T> implements TreeNode<T>{

	private static final long serialVersionUID 	= 480902938041176366L;

	private static final Serializable[] locks = new Serializable[]{
		new Integer(0),
		new Integer(1),
		new Integer(2),
		new Integer(3),
		new Integer(4),
		new Integer(5),
		new Integer(6),
		new Integer(7),
		new Integer(8),
		new Integer(9),
	};
	
	public static int MIN_CHARGROUP    			= 0x5b;

    public static int MAX_CHARGROUP    			= 0x7d;

    public static int LEN_CHARGROUP    			= MAX_CHARGROUP - MIN_CHARGROUP;

    public static int MIN_NUMBERGROUP  			= 0x21;

    public static int MAX_NUMBERGROUP  			= 0x3f;

    public static int LEN_NUMBERGROUP  			= MAX_NUMBERGROUP - MIN_NUMBERGROUP;

	public static int MIN_CHAR2GROUP    		= 0xe0;

	public static int MAX_CHAR2GROUP    		= 0xff;
	
	public static int LEN_CHAR2GROUP    		= MAX_CHAR2GROUP - MIN_CHAR2GROUP;
    
	public static int LEN_NODES        			= LEN_NUMBERGROUP + LEN_CHARGROUP + LEN_CHAR2GROUP; 

    public static int DATA_SIZE        			= LEN_NODES*8 + 16; 

    public static final long MAX_NODES 			= Long.MAX_VALUE / DATA_SIZE;

    private long id;

    private volatile long valueId;

    private long[] nextNodes;

    public CharNode(){
        this.nextNodes  = new long[LEN_NODES];
        this.id         = -1;
        this.valueId    = -1;

        for(int i=0;i<this.nextNodes.length;i++)
                this.nextNodes[i] = -1;
    }

    public void setNext(ReferenceCollection<TreeNode<T>> nodes, Object key, TreeNode<T> node){
    	char c = (Character)key;
        int index = c/* & 0xff*/;

		if(index < MIN_CHARGROUP && index < MIN_NUMBERGROUP && index < MIN_CHAR2GROUP)
			throw new IllegalArgumentException("invalid char: " + id);
		
		if(index > MAX_CHARGROUP && index > MAX_NUMBERGROUP &&  index > MAX_CHAR2GROUP)
			throw new IllegalArgumentException("invalid char: " + id);

		if(index <= MAX_NUMBERGROUP)
			index = index - MIN_NUMBERGROUP;
		else
		if(index <= MAX_CHARGROUP)
			index = LEN_NUMBERGROUP + (index - MIN_CHARGROUP);
		else
			index = LEN_NUMBERGROUP + LEN_CHARGROUP + (index - MIN_CHAR2GROUP);
		
        this.nextNodes[index] = node.getId();
        nodes.set(this.id, this);
    }

    public TreeNode<T> getNext(ReferenceCollection<TreeNode<T>> nodes, Object key) {
    	char c = (Character)key;
        int index = c/* & 0xff*/;

		if(index < MIN_CHARGROUP && index < MIN_NUMBERGROUP && index < MIN_CHAR2GROUP)
			throw new IllegalArgumentException("invalid char: " + id);
		
		if(index > MAX_CHARGROUP && index > MAX_NUMBERGROUP &&  index > MAX_CHAR2GROUP)
			throw new IllegalArgumentException("invalid char: " + id);

		if(index <= MAX_NUMBERGROUP)
			index = index - MIN_NUMBERGROUP;
		else
		if(index <= MAX_CHARGROUP)
			index = LEN_NUMBERGROUP + (index - MIN_CHARGROUP);
		else
			index = LEN_NUMBERGROUP + LEN_CHARGROUP + (index - MIN_CHAR2GROUP);
		
		long nexNode = this.nextNodes[index];

        if(nexNode != -1){
            return nodes.get(nexNode);
        }
        else
            return null;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getValueId() {
        return valueId;
    }

    public T setValue(ReferenceCollection<T> values, T value) {
    	synchronized(locks[(int)(this.id % locks.length)]){
            if(this.valueId == -1){
            	this.valueId = values.insert(value);
                return null;
            }
            else{
        		T old = values.get(valueId); 
                values.set(this.valueId, value);
                return old;
            }
			
		}
    }

    public T removeValue(ReferenceCollection<T> values) {
    	synchronized(locks[(int)(this.id % locks.length)]){
            if(this.valueId != -1){
                return values.set(this.valueId, null);
            }
            else{
            	return null;
            }
    	}
    }

    public T getValue(ReferenceCollection<T> values) {
        if(this.valueId != -1)
            return values.get(this.valueId);
        else
            return null;
    }

	public boolean replaceValue(ReferenceCollection<T> values, T oldValue,
			T value) {
    	synchronized(locks[(int)(this.id % locks.length)]){
            if(this.valueId != -1){
                return values.replace(this.valueId, oldValue, value);
            }
            else{
            	return false;
            }
    	}
	}

	public T replaceValue(ReferenceCollection<T> values, T value) {
    	synchronized(locks[(int)(this.id % locks.length)]){
            if(this.valueId != -1){
                return values.replace(this.valueId, value);
            }
            else{
            	return null;
            }
    	}
	}

	public T putIfAbsentValue(ReferenceCollection<T> values, T value) {
    	synchronized(locks[(int)(this.id % locks.length)]){
            if(this.valueId != -1){
                return values.putIfAbsent(this.valueId, value);
            }
            else{
            	this.valueId = values.insert(value);
            	return null;
            }
    	}
	}

	public boolean removeValue(ReferenceCollection<T> values, T oldValue) {
    	synchronized(locks[(int)(this.id % locks.length)]){
            if(this.valueId != -1){
                return values.remove(this.valueId, oldValue);
            }
            else{
            	return false;
            }
    	}
	}

}
