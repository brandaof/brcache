package org.brandao.brcache;

import java.io.Serializable;

@SuppressWarnings("serial")
class Block 
    implements Serializable{

    public long id;

    public int segment;
    
    public int length;
    
    public long nextBlock;
    
    public byte[] buffer;
    
    public Block(long id, int segment, byte[] data, int length){
        this.id        = id;
        this.segment   = segment;
        this.buffer    = data;
        this.length    = length;
        this.nextBlock = -1;
    }
    
}
