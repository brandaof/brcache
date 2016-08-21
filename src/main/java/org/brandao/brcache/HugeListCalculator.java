package org.brandao.brcache;

class HugeListCalculator {

	public static HugeListInfo calculate(
			long dataBufferSize, long dataPageSize, 
			long blockSize, double dataSwapFactor){
		
    	if(dataBufferSize <= 0)
    		throw new IllegalArgumentException("buffer size <= 0");

    	if(dataPageSize <= 0)
    		throw new IllegalArgumentException("slab size <= 0");

    	if(blockSize <= 0)
    		throw new IllegalArgumentException("block size <= 0");

    	if(dataPageSize > dataBufferSize)
    		throw new IllegalArgumentException("slab size > buffer size");

    	if(blockSize > dataPageSize)
    		throw new IllegalArgumentException("block size > slab size");
    	
    	//Quantidade de blocos na memória.
    	double blocksLength   = dataBufferSize/blockSize;
    	blocksLength          = dataBufferSize%blockSize > 0? blocksLength + 1 : blocksLength;
    	
    	//Quantidade de blocos em uma página
    	double blocksPerSlab  = dataPageSize/blockSize;
    	blocksPerSlab         = dataPageSize%blockSize > 0? blocksPerSlab + 1 : blocksPerSlab;
    	
    	//Fator de páginas. Usado para definir o fator de fragmentação da lista.
    	double slabFactor     = blocksPerSlab/blocksLength;
    	//Quantidade de páginas na memória.
    	//double slabs          = blocksLength/blocksPerSlab;
    	
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
