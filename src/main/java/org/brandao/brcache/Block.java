package org.brandao.brcache;

import java.io.Serializable;

@SuppressWarnings("serial")
class Block 
    implements Serializable{

    public long id;

    public int segment;
    
    public int length;
    
    public int nextBlock;
    
    public RegionMemory buffer;
    
    public Block(long id, int segment, RegionMemory data, int length){
        this.id        = id;
        this.segment   = segment;
        this.buffer    = data;
        this.length    = length;
        this.nextBlock = -1;
    }
    
}
