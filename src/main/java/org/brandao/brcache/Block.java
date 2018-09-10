package org.brandao.brcache;

import java.io.Serializable;

import org.brandao.brcache.memory.RegionMemory;

@SuppressWarnings("serial")
public class Block 
    implements Serializable{

    public long id;

    public int segment;
    
    public int length;
    
    public long nextBlock;
    
    public RegionMemory buffer;
    
    public Block(long id, int segment, RegionMemory data, int length){
        this.id        = id;
        this.segment   = segment;
        this.buffer    = data;
        this.length    = length;
        this.nextBlock = -1;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + segment;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Block other = (Block) obj;
		if (id != other.id)
			return false;
		if (segment != other.segment)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Block [id=" + id + ", segment=" + segment + ", length="
				+ length + ", nextBlock=" + nextBlock + "]";
	}
    
}
