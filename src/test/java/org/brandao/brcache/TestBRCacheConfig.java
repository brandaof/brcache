package org.brandao.brcache;

import org.brandao.brcache.memory.UnsafeMemory;

public class TestBRCacheConfig 
	extends BRCacheConfig{

	private static final long serialVersionUID = -8953971203516129784L;

	public TestBRCacheConfig(){

        this.nodesBufferSize = 1*1024*1024;
        this.nodesPageSize   = 1024;
        this.nodesSwapFactor = 0.3;
        this.indexBufferSize = 32*1024*1024;
        this.indexPageSize   = 1024;
        this.indexSwapFactor = 0.3;
        this.dataBufferSize  = 32*1024*1024;
        this.dataBlockSize   = 1024;
        this.dataPageSize    = 2*1024;
        this.dataSwapFactor  = 0.3;
        this.maxSizeEntry    = 1024*1024;
        this.maxSizeKey      = 100;
        this.swapperThread   = 4;
        this.dataPath        = "/mnt/brcache";
        this.memory          = new UnsafeMemory();//new HeapMemory();
	}
	
}
