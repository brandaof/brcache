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
    	
    	double subLists       = (dataBufferSize / (blockSize*2048L)/*SUBLIST_DATA_SIZE*/);
    	subLists              = subLists > 12? 12 : subLists;
    	subLists              = subLists == 0? 1 : subLists;
    	
    	dataBufferSize        = (long)(dataBufferSize / subLists);
    	
    	//Quantidade de blocos na memória.
    	double blocksLength   = dataBufferSize/blockSize;
    	blocksLength          = dataBufferSize%blockSize > 0? blocksLength + 1 : blocksLength;
    	
    	//Quantidade de blocos em uma página
    	double blocksPerPage  = dataPageSize/blockSize;
    	blocksPerPage         = dataPageSize%blockSize > 0? blocksPerPage + 1 : blocksPerPage;
    	
    	//Fator de páginas. Usado para definir o fator de fragmentação da lista.
    	double pageFactor     = blocksPerPage/blocksLength;
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

        return new HugeListInfo((int)blocksLength, swapFactor, pageFactor, (int)subLists);    			
	}
	
	public static class HugeListInfo{
		
		private int maxCapacityElements;
		
		private double clearFactorElements;
        
		private double fragmentFactorElements;

		private int subLists;
		
		public HugeListInfo(int maxCapacityElements,
				double clearFactorElements, double fragmentFactorElements, int subLists) {
			this.maxCapacityElements = maxCapacityElements;
			this.clearFactorElements = clearFactorElements;
			this.fragmentFactorElements = fragmentFactorElements;
			this.subLists = subLists;
		}

		public int getSubLists() {
			return subLists;
		}

		public void setSubLists(int subLists) {
			this.subLists = subLists;
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

		@Override
		public String toString() {
			return "HugeListInfo [maxCapacityElements=" + maxCapacityElements
					+ ", clearFactorElements=" + clearFactorElements
					+ ", fragmentFactorElements=" + fragmentFactorElements
					+ ", subLists=" + subLists + "]";
		}
        
	}
}
