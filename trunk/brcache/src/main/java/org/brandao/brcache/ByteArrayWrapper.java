package org.brandao.brcache;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ByteArrayWrapper 
    implements Serializable{

    public long id;

    public int segment;
    
    public int length;
    
    public RegionMemory buffer;
    
    public ByteArrayWrapper(long id, int segment, RegionMemory data, int length){
        this.id      = id;
        this.segment = segment;
        this.buffer  = data;
        this.length  = length;
    }
    
}
