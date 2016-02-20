package org.brandao.brcache.collections.fileswapper;

import java.io.IOException;
import java.io.Serializable;

public class IndexKey implements Serializable{

	private static final long serialVersionUID = 3595347631812335263L;

	private static final int[] charMap = new int[256];
	
	static{
		
		for(int i=0;i<charMap.length;i++){
			charMap[i] = -1;
		}
		
		int id = 0;
		
		for(int i='!';i<='@';i++){
			int index = i & 0xff;
			charMap[index] = id++;
		}

		for(int i='[';i<='~';i++){
			int index = i & 0xff;
			charMap[index] = id++;
		}

		for(int i='à';i<='ö';i++){
			int index = i & 0xff;
			charMap[index] = id++;
		}

		for(int i='ù';i<='ý';i++){
			int index = i & 0xff;
			charMap[index] = id++;
		}
		
		charMap['ÿ' & 0xff] = id++;
		
		LEN_NODES = id;
	}

	public static final int LEN_NODES; 
	
	public static final int DATA_SIZE  = LEN_NODES*8 + 18; 
	
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
		int index     = id & 0xff;
		int charMapId = charMap[index];
		
		if(charMapId < 0)
			throw new IllegalArgumentException("invalid char: " + id);
		
		this.nextNodes[charMapId] = node.id;
		this.needUpdate           = true;
	}
	
	public IndexKey getNextNode(char id, SimpleIndexEntityFile session) throws IOException{
		int index     = id & 0xff;
		int charMapId = charMap[index];
		
		if(charMapId < 0)
			throw new IllegalArgumentException("invalid char: " + id);
		
		long nexNode = this.nextNodes[charMapId];
		
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
