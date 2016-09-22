package org.brandao.brcache;

/**
 * Descreve as estratégias de acesso à memória.
 * 
 * @author Brandao.
 *
 */
public enum MemoryAccessStrategy {

	/**
	 * Os dados são gravados no heap. 
	 * <p>Equivale ao trecho a seguir:</p>
	 * <pre>segmentMemory = new byte[capacity];</pre>
	 */
	HEAP,
	
	/**
	 * Os dados são gravados fora do heap. 
	 * <p>Equivale ao trecho a seguir:</p>
	 * <pre>segmentMemory = ByteBuffer.allocateDirect(capacity);</pre>
	 */
	DIRECT,
	
	/**
	 * Os dados são gravados fora do heap. 
	 * <p>Equivale ao trecho a seguir:</p>
	 * <pre>segmentMemory = Unsafe.allocateMemory(capacity);</pre>
	 */
	UNSAFE;
	
}
