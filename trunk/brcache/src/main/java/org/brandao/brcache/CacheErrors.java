package org.brandao.brcache;

public class CacheErrors {

	public static final CacheError ERROR_1007 = new CacheError(1007, "Data is very large!");

	public static final CacheError ERROR_1008 = new CacheError(1008, "Key is very large!");

	public static final CacheError ERROR_1010 = new CacheError(1010, "Commit in progress!");

	public static final CacheError ERROR_1011 = new CacheError(1011, "Transaction has been rolled back!");

	public static final CacheError ERROR_1012 = new CacheError(1012, "Transaction has been commited!");

	public static final CacheError ERROR_1013 = new CacheError(1013, "Transaction not started!");

	public static final CacheError ERROR_1014 = new CacheError(1014, "cache write error!");

	public static final CacheError ERROR_1016 = new CacheError(1016, "Transaction has been started!");

	public static final CacheError ERROR_1018 = new CacheError(1018, "Rollback error!");

	public static final CacheError ERROR_1019 = new CacheError(1019, "Commit error!");

	public static final CacheError ERROR_1020 = new CacheError(1020, "Store item error!");
	
	public static final CacheError ERROR_1021 = new CacheError(1021, "Get item error!");

	public static final CacheError ERROR_1022 = new CacheError(1022, "Fails to acquire the lock!");

	public static final CacheError ERROR_1023 = new CacheError(1023, "Fails to execute action!");
	
}
