package org.brandao.brcache.collections.fileswapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataChain {

	private static final List<Long> EMPTY_LIST = new ArrayList<Long>();
        
        private static final byte[] EMPTY_DATA = new byte[0];
	
	public static Long save(List<DataBlock> list, DataBlockEntityFile handler) throws IOException{
		return update(null, list, handler);
	}

	public static Long update(Long firstId, List<DataBlock> list, DataBlockEntityFile handler) throws IOException{
		
		handler.seek(0);
		DataBlock free = handler.read();
		
		if(free.getNextBlock() == -1)
			free = null;
		else{
			handler.seek(free.getNextBlock());
			free = handler.read();
		}
		 
		Long lastId  = null;
		
		List<Long> freeList = firstId == null? EMPTY_LIST : getUsedIds(firstId, handler);
		
		DataBlock[] arr = list.toArray(new DataBlock[]{});
		for(int i=arr.length-1;i>=0;i--){
			DataBlock entity = arr[i];
			
			Long id = freeList.isEmpty()? null : freeList.remove(0);
			entity.setNextBlock(lastId == null? -1 : lastId);
			
			if(id != null){
				lastId = id;
				entity.setId(id);
				handler.seek(id);
				handler.write(entity);
			}
			else{
				if(free != null){
					entity.setId(free.getId());
					handler.seek(entity.getId());
					handler.write(entity);
					lastId = entity.getId();
					
					if(free.getNextBlock() != -1){
						handler.seek(free.getNextBlock());
						free = handler.read();
					}
					else{
						handler.seek(0);
						handler.write(free);
						free = null;
					}
				}
				else{
					handler.seek(handler.length());
					entity.setId(handler.getOffset());
					handler.write(entity);
					lastId = entity.getId();
				}
			}
			
		}
		
		if(free != null){
			handler.seek(0);
			free.setNextBlock(free.getId());
                        free.setData(EMPTY_DATA);
                        free.setId(-1);
			handler.write(free);
		}
		
		remove(handler, freeList);		
		
		return lastId;
	}
	
	
	public static void remove(Long firstId, DataBlockEntityFile handler) throws IOException{
		List<Long> freeList = getUsedIds(firstId, handler);
		remove(handler, freeList);		
	}

	public static void remove(DataBlockEntityFile handler, List<Long> freeList) throws IOException{
            
            if(freeList.isEmpty()){
                return;
            }
            
		handler.seek(0);
		DataBlock firstBlock = handler.read();
		long lastBlockId = firstBlock.getNextBlock();
                
		for(Long id: freeList){
			DataBlock freeBlock = new DataBlock();
			freeBlock.setData(EMPTY_DATA);
			freeBlock.setId(id);
			freeBlock.setNextBlock(lastBlockId);
			
			lastBlockId = id;
			handler.seek(id);
			handler.write(freeBlock);
		}

		firstBlock.setNextBlock(lastBlockId);
		handler.seek(0);
		handler.write(firstBlock);
	}
	
	public static List<DataBlock> reload(Long firstId, DataBlockEntityFile handler) throws IOException{
		List<DataBlock> list = new ArrayList<DataBlock>();
		
		DataBlock chain = null;
		
		if(firstId != null){
			handler.seek(firstId);
			chain = handler.read();
		}
		
		while(chain != null){
			list.add(chain);

			Long nextId = chain.getNextBlock();
			
			if(nextId == -1)
				break;
			
			handler.seek(nextId);
			chain = handler.read();
		}
		
		return list;
	}
	
	private static List<Long> getUsedIds(Long firstId, DataBlockEntityFile handler) throws IOException{
		List<Long> list = new ArrayList<Long>();
		
		DataBlock chain = null;
		
		if(firstId != null){
			handler.seek(firstId);
			chain = handler.read();
		}
		
		while(chain != null){
			list.add(chain.getId());

			Long nextId = chain.getNextBlock();

			if(nextId == -1)
				break;
			
			handler.seek(nextId);
			chain = handler.read();
			
		}
		
		return list;
	}

}
