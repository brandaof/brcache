package org.brandao.brcache.server.error;

public class ServerErrorException 
	extends Exception{

	private static final long serialVersionUID = -4922888278517137089L;
	
	private ServerError error;
	
	public ServerErrorException(ServerError error, String message) {
		super(message);
		this.error = error;
	}

	public ServerErrorException(ServerError error, String message, Throwable cause) {
		super(message, cause);
		this.error = error;
	}
	
	public ServerError getError() {
		return error;
	}

}
