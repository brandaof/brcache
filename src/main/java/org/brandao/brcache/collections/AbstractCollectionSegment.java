/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.collections;

import java.io.*;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Brandao
 */
abstract class AbstractCollectionSegment<I,T> 
    implements CollectionSegment<I>, 
    /*CollectionSegmentSwapper<T>,*/ Runnable, Serializable{
    
	private static final long serialVersionUID = 7817500681111470845L;

	private static final int MAX_ITENS_PER_SEGMENT = 300;
	
    protected Segment<T>[] segments;

    private transient File path;
    
    private transient boolean hasCreatePath;
    
    private String id;
    
    private int maxCapacity;
    
    private int maxSegmentCapacity;
    
    private double fragmentSize;
    
    private double clearFactor;
    
    protected boolean readOnly;
    
    private Swapper swap;

    private boolean forceSwap;
    
    private BlockingQueue<Entry<T>> swapCandidates;
    
    private transient Thread[] swapperThreads;
    
    private boolean live;
    
    private volatile long onMemory;
    
    @SuppressWarnings("unchecked")
	public AbstractCollectionSegment(
            String id, int maxCapacity, double clearFactor,
            double fragmentFactor,
            Swapper swap,
            int quantitySwaperThread) {

        this.id                  = id;
        this.fragmentSize        = (int)(maxCapacity * fragmentFactor);
        this.maxCapacity         = maxCapacity;
        this.clearFactor         = clearFactor;
        this.maxSegmentCapacity  = (int)(maxCapacity/fragmentSize);
        this.swapCandidates      = new LinkedBlockingQueue<Entry<T>>();
        this.readOnly            = false;
        this.swap                = swap;
        this.forceSwap           = true;
        this.live                = true;
        this.segments            = new Segment[maxCapacity/MAX_ITENS_PER_SEGMENT + (maxCapacity % MAX_ITENS_PER_SEGMENT != 0? 1 : 0)];
        this.onMemory            = 0;
        swapperThreads           = new Thread[quantitySwaperThread];
        this.swap.setId(this.id);
        
        int countMaxCapacity = maxCapacity;
        for(int i=0;i<this.segments.length;i++){
        	this.segments[i] = new Segment<T>(
        			swap, forceSwap, 
        			(countMaxCapacity - MAX_ITENS_PER_SEGMENT) > MAX_ITENS_PER_SEGMENT? 
        					MAX_ITENS_PER_SEGMENT : 
        					countMaxCapacity,
					readOnly);
        	
        	countMaxCapacity = countMaxCapacity - MAX_ITENS_PER_SEGMENT;
        }
        
        for(int i=0;i<swapperThreads.length;i++){
        	SwapperThread swapperThread = new SwapperThread(swapCandidates);
        	swapperThreads[i] = new Thread(swapperThread);
        	swapperThreads[i].start();
        }
        
        Thread clearThread = new Thread(this);
        clearThread.start();
    }
    
    /* ------ */
    
    protected void add(Entry<T> item) {
    	this.getSegment(item.getIndex()).add(item);
        this.onMemory++;
    }

    protected Entry<T> remove(Entry<T> item){
        Entry<T> e = this.getSegment(item.getIndex()).remove(item);
        this.onMemory--;
        return e;
    }
    
    protected Entry<T> getEntry(long index) {
    	return this.getSegment(index).getEntry(index);
    }
    
    public Entry<T> reload(Entry<T> entity){
    	return this.getSegment(entity.getIndex()).reload(entity);
    }
    
    /* ------ */
    
    public void run(){
        while(this.live){
            try{
                Thread.sleep(10000);
            	clearLimit();
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    protected Segment<T> getSegment(long value){
    	return this.segments[(int)(value % this.segments.length)];
    }
    
    public boolean isLive() {
		return live;
	}

    protected void clearLimit() {
    	double i = maxSegmentCapacity * clearFactor;
        double limit = maxSegmentCapacity - i;
        
        if (maxSegmentCapacity > 0 && onMemory > limit) {
        	int count = 0;
        	int free  = 0;
            do{
            	free  = 0;
	        	for(Segment<T> seg: this.segments){
	        		if(seg.swapNextCandidate()){
	        			count++;
	        		}
	        		else{
	        			free++;
	        		}
	        	}
            }
            while(count < i && free != this.segments.length);
        }
        
    }
    
    protected boolean needSwap(){
    	return maxSegmentCapacity > 0 && onMemory > maxSegmentCapacity - 2;    	
    }
    
    public void flush(){
    	for(Segment<T> seg: this.segments){
    		while(seg.swapNextCandidate());
    	}
    }
    
    /* ------ */
    
    public Map<Long, Entry<T>> getSegments() {
        throw new UnsupportedOperationException();
    }

    public File getPath() {
        return path;
    }

    public void setPath(File path) {
        this.path = path;
    }

    public boolean isHasCreatePath() {
        return hasCreatePath;
    }

    public void setHasCreatePath(boolean hasCreatePath) {
        this.hasCreatePath = hasCreatePath;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

    public void setMaxCapacity(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public int getMaxSegmentCapacity() {
        return maxSegmentCapacity;
    }

    public void setMaxSegmentCapacity(int maxSegmentCapacity) {
        this.maxSegmentCapacity = maxSegmentCapacity;
    }

    public double getFragmentSize() {
        return fragmentSize;
    }

    public void setFragmentSize(float fragmentSize) {
        this.fragmentSize = fragmentSize;
    }

    public double getClearFactor() {
        return clearFactor;
    }

    public void setClearFactor(float clearFactor) {
        this.clearFactor = clearFactor;
    }

    public void setReadOnly(boolean value){
        this.readOnly = value;
    }
    
    public boolean isReadOnly(){
        return this.readOnly;
    }

    public boolean isForceSwap() {
        return forceSwap;
    }

    public void setForceSwap(boolean forceSwap) {
        this.forceSwap = forceSwap;
    }

    public void clear(){
    	for(Segment<T> seg: this.segments){
    		seg.clear();
    	}
    	this.swapCandidates.clear();
        this.swap.clear();
    }
    
    public void destroy(){
    	
    	try{
	    	for(Thread st: this.swapperThreads){
	    		st.interrupt();
	    	}
    	}
    	finally{
	    	this.live      = false;
	    	this.swapCandidates.clear();
	        this.swap.destroy();
	        
	    	for(Segment<T> seg: this.segments){
	    		seg.clear();
	    	}
    	}
    	
    }

    public class SwapperThread implements Runnable{

    	private BlockingQueue<Entry<T>> itens;
    	
    	public SwapperThread(BlockingQueue<Entry<T>> itens){
    		this.itens = itens;
    	}
    	
    	public void run() {
    		while(AbstractCollectionSegment.this.live){
    			try{
    				Entry<T> entry = itens.take();
    				AbstractCollectionSegment.this.getSegment(entry.getIndex()).swap(entry);
    			}
    			catch(Throwable e){
    				if(!(e instanceof InterruptedException)){
    					e.printStackTrace();
    				}
    			}
    		}
    		
    	}
    	
    }
    
}
