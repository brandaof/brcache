package org.brandao.brcache.collections.treehugemap;

import java.util.List;

public class CharNode<T> implements TreeNode<T>{

	private static final long serialVersionUID 	= 480902938041176366L;

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

    public void setNext(List<TreeNode<T>> nodes, Object key, TreeNode<T> node){
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
        nodes.set((int)this.id, this);
    }

    public TreeNode<T> getNext(List<TreeNode<T>> nodes, Object key) {
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
            return nodes.get((int)nexNode);
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

    public void setValue(List<T> values, T value) {
        if(this.valueId == -1){
            if(this.valueId == -1){
                values.add(value);
                this.valueId = values.size() - 1;
            }
            else
                values.set((int)this.valueId, value);
        }
        else
            values.set((int)this.valueId, value);
    }

    public void removeValue(List<T> values) {
        if(this.valueId != -1){
            values.set((int)this.valueId, null);
        }
    }

    public T getValue(List<T> values) {
        if(this.valueId != -1)
            return values.get((int)this.valueId);
        else
            return null;
    }

}
