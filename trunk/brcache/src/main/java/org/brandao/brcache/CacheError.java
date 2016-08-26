package org.brandao.brcache;

/**
 * Descreve um erro no cache. 
 * @author Brandao.
 *
 */
public class CacheError {

	private final int id;
	
	private final String message;

	/**
	 * Cria um novo erro com uma identificação e mensagem específica.
	 * @param id identificação.
	 * @param message mensagem.
	 */
	public CacheError(int id, String message) {
		this.id = id;
		this.message = message;
	}

	/**
	 * Obtém a identificação.
	 * @return identificação.
	 */
	public int getId() {
		return id;
	}

	/**
	 * Obtém a mensagem do erro.
	 * @return mensagem.
	 */
	public String getMessage() {
		return message;
	}
	
	/**
	 * Obtém a descrição do erro.
	 * @param params
	 * @return
	 */
	public String getString(Object ... params){
		return "ERROR " + this.id + ": " + String.format(this.message, (Object[])params);
	}
	
}
