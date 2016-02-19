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
	
	public static int MIN_CHAR2GROUP    = 0xe0;

	public static int MAX_CHAR2GROUP    = 0xff;
	
	public static int LEN_CHAR2GROUP    = MAX_CHAR2GROUP - MIN_CHAR2GROUP;
	
	public static int LEN_NODES        = LEN_NUMBERGROUP + LEN_CHARGROUP + LEN_CHAR2GROUP; 

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
		int index = id & 0xff;
		
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
		
		this.nextNodes[index] = node.id;
		this.needUpdate       = true;
	}
	
	public IndexKey getNextNode(char id, SimpleIndexEntityFile session) throws IOException{
		int index = id & 0xff;
		
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
