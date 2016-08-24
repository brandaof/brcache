package org.brandao.brcache.server.error;

public class ServerError {

	private final int id;
	
	private final String message;

	public ServerError(int id, String message) {
		this.id = id;
		this.message = message;
	}

	public int getId() {
		return id;
	}

	public String getMessage() {
		return message;
	}
	
	public String toString(Object ... params){
		return "ERROR " + this.id + ": " + String.format(this.message, params);
	}
	
}
