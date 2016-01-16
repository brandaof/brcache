package org.brandao.brcache.collections;

import java.util.concurrent.BlockingQueue;

public class SwapperThread<T> implements Runnable{

	private BlockingQueue<Entry<T>> itens;
	
	private CollectionSegmentSwapper<T> swapper;
	
	public SwapperThread(BlockingQueue<Entry<T>> itens, CollectionSegmentSwapper<T> swapper){
		this.itens = itens;
		this.swapper = swapper;
	}
	
	public void run() {
		while(true){
			try{
				Entry<T> segment = itens.take();
				swapper.swapOnDisk(segment);
			}
			catch(InterruptedException e){
				e.printStackTrace();
			}
		}
		
	}

}
