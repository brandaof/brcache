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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CacheError other = (CacheError) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
}
