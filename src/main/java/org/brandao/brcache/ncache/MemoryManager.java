package org.brandao.brcache.ncache;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.brandao.brcache.collections.Entry;
import org.brandao.brcache.collections.Swapper;

public class MemoryManager {

	private final ByteBuffer memory;
	
	private Map<Integer,Integer> mappedSlab;
	
	private final int slabSize;
	
	private final int totalSlab;
	
    private final BlockingQueue<Integer> freeSlabsOnMemory;

    private final BlockingQueue<Integer> usedSlabsOnMemory;
    
    private final Swapper<byte[]> swapper;
    
    private final int maxSwapSlab;
    
    private int lastSegment;
    
    private final BlockingQueue<Integer> freeSegments;
    
    private final int segmentSize;
    
    public MemoryManager(
    		long slabSize, long totalMemory, 
    		long swapFactor, long segmentSize, Swapper<byte[]> swapper){
    	
    	this.segmentSize        = (int)segmentSize;
    	this.slabSize           = (int)slabSize;
    	this.memory             = ByteBuffer.allocateDirect((int)totalMemory);
    	this.freeSlabsOnMemory  = new LinkedBlockingQueue<Integer>();
    	this.usedSlabsOnMemory  = new LinkedBlockingQueue<Integer>();
    	this.freeSegments       = new LinkedBlockingQueue<Integer>();
    	this.swapper            = swapper;
    	this.totalSlab          = (int)(totalMemory/slabSize);
    	this.maxSwapSlab        = (int)(this.totalSlab*swapFactor);
    	this.lastSegment        = 0;
    	this.mappedSlab      = new HashMap<Integer, Integer>();
    	
    	for(int i=0;i<this.totalSlab;i++)
    		this.freeSlabsOnMemory.add(i);
    	
    	this.swapper.setId("data");
    	
        Thread[] clearThread = new Thread[4];
        
        for(int i=0;i<clearThread.length;i++){
            clearThread[i] =
                new Thread(){
                  
                    public void run(){
                        while(true){
                            try{
                            	swap();
                                Thread.sleep(1000);
                            }
                            catch(Exception e){
                                e.printStackTrace();
                            }
                        }
                    }
                    
                };
            
            clearThread[i].start();
        }
    	
    }
    
	public MemoryEntry write(InputStream in) throws IOException{
		List<Integer> segs = new ArrayList<Integer>();
		long size = 0;
		try{
			byte[] buffer = new byte[this.segmentSize];
			int len;
			
			while((len = in.read(buffer, 0, buffer.length)) != -1){
				size += len;
				int segment = this.write(buffer, 0, len);
				segs.add(segment);
			}
			return new MemoryEntry(size, segs.toArray(new Integer[]{}));
		}
		catch(Throwable e){
			for(Integer i: segs)
				this.freeSegments.add(i);
			throw new IOException(e);
		}
	}

	public synchronized byte[] read(int segment) throws InterruptedException{
		
		Integer segmentOnMemory = this.mappedSlab.get(segment);
		
		if(segmentOnMemory == null)
			segmentOnMemory = this.swapOnMemory(segment);
		
		byte[] data = new byte[this.slabSize];
		this.memory.position(segmentOnMemory*this.slabSize);
		this.memory.get(data, 0, data.length);
		return data;
	}

	public void releaseSegment(int segment){
		Integer segmentOnMemory = this.mappedSlab.get(segment);
		
		if(segmentOnMemory != null){
			this.freeSlabsOnMemory.add(segmentOnMemory);
			this.mappedSlab.remove(segment);
		}
		
		this.freeSegments.add(segment);
	}
	
	/**
	 * Transfere um segmento do disco para a memória e retorna 
	 * o segmento da memória que foi alocado. 
	 * @param segment Segmento dos dados.
	 * @return Segmento da memória.
	 * @throws InterruptedException 
	 */
	protected int swapOnMemory(int segment) throws InterruptedException{
		
		int segmentOnMemory = this.getFreeSlabOnMemory();
		
		this.mappedSlab.put(segment, segmentOnMemory);
		this.usedSlabsOnMemory.put(segment);
		
		Entry<byte[]> entry = this.swapper.getItem(segment);
		byte[] data = entry.getItem();
		this.memory.position(segmentOnMemory*this.slabSize);
		this.memory.put(data, 0, data.length);
		return segmentOnMemory;
	}

	/**
	 * Transfere um segmento da memória para o disco disco para a memória e retorna 
	 * o segmento da memória que foi alocado. 
	 * @param segment Segmento dos dados.
	 * @return Segmento da memória.
	 * @throws InterruptedException 
	 */
	protected int swapOnDisk(int segment) throws InterruptedException{
		Integer segmentOnMemory = this.mappedSlab.get(segment);
		byte[] data = new byte[this.slabSize];
		this.memory.position(segmentOnMemory*this.slabSize);
		this.memory.get(data, 0, data.length);
		
		Entry<byte[]> entry = new Entry<byte[]>(segment, data);
		
		this.swapper.sendItem(segment, entry);
		
		this.freeSlabsOnMemory.put(segmentOnMemory);
		this.mappedSlab.remove(segment);
		return segmentOnMemory;
	}
	
	protected int allocSegmentOnMemory(int segment) throws InterruptedException{
		int slab = (segment*this.segmentSize);
		int segmentOnMemory = this.getFreeSlabOnMemory();
		this.mappedSlab.put(segment, segmentOnMemory);
		this.usedSlabsOnMemory.put(segment);
		return segmentOnMemory;
	}
	
	protected synchronized int write(byte[] data, int offset, int len) throws InterruptedException{
		Integer segment = this.freeSegments.poll();
		
		if(segment == null)
			segment = this.lastSegment++;

		int segmentOnMemory = this.allocSegmentOnMemory(segment);
		this.memory.position(segmentOnMemory*this.slabSize);
		this.memory.put(data, offset, len);
		return segment;
	}

	protected Integer getFreeSlabOnMemory() throws InterruptedException{
		Integer segment = this.freeSlabsOnMemory.poll();
		if(segment == null){
			this.swap();
			segment = this.freeSlabsOnMemory.poll();
		}
		
		return segment;
	}
	
	protected void swap() throws InterruptedException{
		
		if(this.freeSlabsOnMemory.size() > this.maxSwapSlab)
			return;
		
		int it = (this.maxSwapSlab/4)+1;
		
		for(int i=0;i<it;i++){
			Integer segment = this.usedSlabsOnMemory.poll();
			
			if(segment == null)
				break;
			
			this.swapOnDisk(segment);
		}
	}
	
}
