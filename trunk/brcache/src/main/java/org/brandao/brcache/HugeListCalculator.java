package org.brandao.brcache;

public class HugeListCalculator {

	public static HugeListInfo calculate(
			long dataBufferSize, long dataSlabSize, 
			long blockSize, double dataSwapFactor){
		
    	if(dataBufferSize <= 0)
    		throw new IllegalArgumentException("buffer size <= 0");

    	if(dataSlabSize <= 0)
    		throw new IllegalArgumentException("slab size <= 0");

    	if(blockSize <= 0)
    		throw new IllegalArgumentException("block size <= 0");

    	if(dataSlabSize > dataBufferSize)
    		throw new IllegalArgumentException("slab size > buffer size");

    	if(blockSize > dataSlabSize)
    		throw new IllegalArgumentException("block size > slab size");
    	
    	//Quantidade de blocos na memória.
    	double blocksLength   = dataBufferSize/blockSize;
    	blocksLength          = dataBufferSize%blockSize > 0? blocksLength + 1 : blocksLength;
    	
    	//Quantidade de blocos em uma laje
    	double blocksPerSlab  = dataSlabSize/blockSize;
    	blocksPerSlab         = dataSlabSize%blockSize > 0? blocksPerSlab + 1 : blocksPerSlab;
    	
    	//Fator de laje. Usado para definir o fator de fragmentação da lista.
    	double slabFactor     = blocksPerSlab/blocksLength;
    	//Quantidade de lajes na memória.
    	double slabs          = blocksLength/blocksPerSlab;
    	//Tamanho do buffer usado para fazer a permuta.
    	double swapBufferSize = dataBufferSize*dataSwapFactor;
    	//Quantidade de blocos que sofrerão permuta.
    	double swapBlocks     = swapBufferSize/blockSize;
    	swapBlocks            = swapBufferSize%blockSize > 0? swapBlocks + 1 : swapBlocks;
    	//Fator de permuta. Usado para definir o fator de permuta da lista.
        double swapFactor     = swapBlocks/blocksLength;
        
    	if(swapBlocks <= 0)
    		throw new IllegalArgumentException("swap factor is very little");
        
        return new HugeListInfo((int)blocksLength, swapFactor, slabFactor);    			
	}
	
	public static class HugeListInfo{
		
		private int maxCapacityElements;
		
		private double clearFactorElements;
        
		private double fragmentFactorElements;

		public HugeListInfo(int maxCapacityElements,
				double clearFactorElements, double fragmentFactorElements) {
			this.maxCapacityElements = maxCapacityElements;
			this.clearFactorElements = clearFactorElements;
			this.fragmentFactorElements = fragmentFactorElements;
		}

		public int getMaxCapacityElements() {
			return maxCapacityElements;
		}

		public void setMaxCapacityElements(int maxCapacityElements) {
			this.maxCapacityElements = maxCapacityElements;
		}

		public double getClearFactorElements() {
			return clearFactorElements;
		}

		public void setClearFactorElements(double clearFactorElements) {
			this.clearFactorElements = clearFactorElements;
		}

		public double getFragmentFactorElements() {
			return fragmentFactorElements;
		}

		public void setFragmentFactorElements(double fragmentFactorElements) {
			this.fragmentFactorElements = fragmentFactorElements;
		}
        
	}
}
