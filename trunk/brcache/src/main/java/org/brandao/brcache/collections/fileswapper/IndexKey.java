package org.brandao.brcache.collections.fileswapper;

import java.io.IOException;
import java.io.Serializable;

public class IndexKey implements Serializable{

	private static final long serialVersionUID = 3595347631812335263L;

	public static int MIN_CHARGROUP    = 0x5b;

	public static int MAX_CHARGROUP    = 0x7d;
	
	public static int LEN_CHARGROUP    = MAX_CHARGROUP - MIN_CHARGROUP;

	public static int MIN_NUMBERGROUP  = 0x21;

	public static int MAX_NUMBERGROUP  = 0x3f;
	
	public static int LEN_NUMBERGROUP  = MAX_NUMBERGROUP - MIN_NUMBERGROUP;
	
	public static int LEN_NODES        = LEN_NUMBERGROUP + LEN_CHARGROUP; 

	public static int DATA_SIZE        = LEN_NODES*8 + 18; 
	
	public static final long MAX_NODES = Long.MAX_VALUE / DATA_SIZE;

	private long id;
	
	private long reference;
	
	private long[] nextNodes;
	
	private boolean needUpdate;
	
	public IndexKey(){
		this.nextNodes  = new long[LEN_NODES];
		this.id         = -1;
		this.reference  = -1;
		this.needUpdate = false;
		
		for(int i=0;i<this.nextNodes.length;i++)
			this.nextNodes[i] = -1;
	}
	
	public void setNextNode(char id, IndexKey node){
		int index = (byte)id;
		
		if(index < MIN_CHARGROUP && index < MIN_NUMBERGROUP)
			throw new IllegalArgumentException("invalid char: " + id);
		
		if(index > MAX_CHARGROUP && index > MAX_NUMBERGROUP)
			throw new IllegalArgumentException("invalid char: " + id);
		
		if(index <= MAX_NUMBERGROUP)
			index = MAX_NUMBERGROUP - index;
		else
			index = LEN_NUMBERGROUP + MAX_CHARGROUP - index;
		
		this.nextNodes[index] = node.id;
		this.needUpdate       = true;
	}
	
	public IndexKey getNextNode(char id, SimpleIndexEntityFile session) throws IOException{
		int index = (byte)id;
		
		if(index < MIN_CHARGROUP && index < MIN_NUMBERGROUP)
			throw new IllegalArgumentException("invalid char: " + id);
		
		if(index > MAX_CHARGROUP && index > MAX_NUMBERGROUP)
			throw new IllegalArgumentException("invalid char: " + id);
		
		if(index <= MAX_NUMBERGROUP)
			index = MAX_NUMBERGROUP - index;
		else
			index = LEN_NUMBERGROUP + MAX_CHARGROUP - index;
		
		long nexNode = this.nextNodes[index];
		
		if(nexNode != -1){
			session.seek(nexNode);
			return (IndexKey) session.read();
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

	public long getReference() {
		return reference;
	}

	public void setReference(long reference) {
		this.reference  = reference;
		this.needUpdate = true;
	}

	public long[] getNextNodes() {
		return nextNodes;
	}

	public void setNextNodes(long[] nextNodes) {
		this.nextNodes = nextNodes;
	}

	public boolean isNeedUpdate() {
		return needUpdate;
	}

	public void setNeedUpdate(boolean needUpdate) {
		this.needUpdate = needUpdate;
	}

}
