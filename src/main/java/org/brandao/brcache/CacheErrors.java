package org.brandao.brcache;

public class CacheErrors {

	public static final CacheError ERROR_1007 = new CacheError(1007, "Data is very large!");

	public static final CacheError ERROR_1008 = new CacheError(1008, "Key is very large!");

	public static final CacheError ERROR_1009 = new CacheError(1009, "Transaction not supported!");

	public static final CacheError ERROR_1010 = new CacheError(1010, "Commit in progress!");

	public static final CacheError ERROR_1011 = new CacheError(1011, "Transaction has been rolled back!");

	public static final CacheError ERROR_1012 = new CacheError(1012, "Transaction has been commited!");

	public static final CacheError ERROR_1013 = new CacheError(1013, "Transaction not started!");

	public static final CacheError ERROR_1014 = new CacheError(1014, "cache write error!");

	public static final CacheError ERROR_1015 = new CacheError(1015, "Unknow error!");

	public static final CacheError ERROR_1016 = new CacheError(1016, "Transaction has been started!");

	public static final CacheError ERROR_1018 = new CacheError(1018, "Rollback error!");

	public static final CacheError ERROR_1019 = new CacheError(1019, "Commit error!");

	
}
