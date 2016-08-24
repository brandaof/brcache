package org.brandao.brcache;

public class CacheError {

	private final int id;
	
	private final String message;

	public CacheError(int id, String message) {
		this.id = id;
		this.message = message;
	}

	public int getId() {
		return id;
	}

	public String getMessage() {
		return message;
	}
	
	public String getString(Object ... params){
		return "ERROR " + this.id + ": " + String.format(this.message, (Object[])params);
	}
	
}
