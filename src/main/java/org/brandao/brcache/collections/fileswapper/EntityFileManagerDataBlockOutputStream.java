package org.brandao.brcache.collections.fileswapper;

import java.util.Arrays;

import org.brandao.entityfilemanager.EntityFile;
import org.brandao.entityfilemanager.EntityFileManager;

public class EntityFileManagerDataBlockOutputStream 
	extends AbstractDataBlockOutputStream{

	private long[] free;

	private int lenFree;
	
	private int offFree;
	
	private long[] segments;
	
	private int len;
	
	private EntityFile<DataBlock> dataBlockEntityFile;

	private EntityFile<DataBlockGroup> dataBlockGroupEntityFile;
	
	private long lastBlock;
	
	public EntityFileManagerDataBlockOutputStream(int blockSize) {
		super(blockSize);
		this.lastBlock = -1;
	}

	@Override
	protected void addBlock(DataBlock dataBlock) {
		long segID;
		
		if(lenFree > 0 && offFree < lenFree){
			segID = free[offFree++];
			
		}
		else{
			
		}
	}
	
	private void addSegment(long id){
		
		if(len == segments.length){
			DataBlockGroup dataBlockGroup = new DataBlockGroup();
			dataBlockGroup.setLen(len);
			dataBlockGroup.setSegments(segments);
			dataBlockGroup.setNextGroup(lastBlock);
			lastBlock = dataBlockGroupEntityFile.insert(dataBlockGroup);
			segments[]
		}
		
		segments[len++] = id;
	}

}
