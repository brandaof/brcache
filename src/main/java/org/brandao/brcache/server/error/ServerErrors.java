package org.brandao.brcache.server.error;

import java.util.HashMap;
import java.util.Map;

import org.brandao.brcache.StorageException;
import org.brandao.brcache.tx.TransactionException;

public class ServerErrors {

	public static final ServerError ERROR_1001 = new ServerError(1001, "The command %s not recognized!");
	
	public static final ServerError ERROR_1002 = new ServerError(1002, "Command not informed!");

	public static final ServerError ERROR_1003 = new ServerError(1003, "% is invalid!");

	public static final ServerError ERROR_1004 = new ServerError(1004, "Bad command syntax error!");

	public static final ServerError ERROR_1005 = new ServerError(1005, "internal error!");

	public static final ServerError ERROR_1006 = new ServerError(1006, "Unknow error!");

	public static final ServerError ERROR_1007 = new ServerError(1007, "Data is very large!");

	public static final ServerError ERROR_1008 = new ServerError(1008, "Key is very large!");

	public static final ServerError ERROR_1009 = new ServerError(1009, "Transaction not supported!");

	public static final ServerError ERROR_1010 = new ServerError(1010, "Commit in progress!");

	public static final ServerError ERROR_1011 = new ServerError(1011, "Transaction has been rolled back!");

	public static final ServerError ERROR_1012 = new ServerError(1012, "Transaction has been commited!");

	public static final ServerError ERROR_1013 = new ServerError(1012, "Transaction not started!");
	
	private static final Map<ErrorKey, ServerError> mappedErros = new HashMap<ErrorKey, ServerError>();
	
	static{
		mappedErros.put(new ErrorKey("data is very large", StorageException.class), ERROR_1007);
		mappedErros.put(new ErrorKey("key is very large", StorageException.class), ERROR_1008);
		mappedErros.put(new ErrorKey("transaction has been started", IllegalStateException.class), ERROR_1009);
		mappedErros.put(new ErrorKey("commit in progress", IllegalStateException.class), ERROR_1010);
		mappedErros.put(new ErrorKey("transaction has been rolled back", TransactionException.class), ERROR_1011);
		mappedErros.put(new ErrorKey("transaction has been commited", TransactionException.class), ERROR_1012);
		mappedErros.put(new ErrorKey("transaction not started", TransactionException.class), ERROR_1013);
	}
	
	public static ServerError getError(String message, Class<?> type){
		
		if(message != null){
			ServerError error = mappedErros.get(new ErrorKey(message.toLowerCase(),type));
			return error == null? ERROR_1006 : error;
		}
		else{
			return ERROR_1005;
		}
	}
	
	private static class ErrorKey{
		
		private String message;
		
		private Class<?> type;

		public ErrorKey(String message, Class<?> type) {
			this.message = message;
			this.type = type;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((message == null) ? 0 : message.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
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
			ErrorKey other = (ErrorKey) obj;
			if (message == null) {
				if (other.message != null)
					return false;
			} else if (!message.equals(other.message))
				return false;
			if (type == null) {
				if (other.type != null)
					return false;
			} else if (!type.equals(other.type))
				return false;
			return true;
		}
		
	}
}
