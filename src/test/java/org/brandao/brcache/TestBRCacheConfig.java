package org.brandao.brcache;

import java.io.File;

import org.brandao.brcache.memory.HeapMemory;
import org.brandao.entityfilemanager.EntityFileManager;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.EntityFileManagerImp;
import org.brandao.entityfilemanager.EntityFileTransactionFactory;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.LockProviderImp;
import org.brandao.entityfilemanager.TransactionLog;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManagerConfigurer;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManagerImp;
import org.brandao.entityfilemanager.tx.TransactionLogImp;
import org.brandao.entityfilemanager.tx.async.AsyncEntityFileTransactionFactory;
import org.brandao.entityfilemanager.tx.async.AsyncRecoveryTransactionLog;

public class TestBRCacheConfig 
	extends BRCacheConfig{

	private static final long serialVersionUID = -8953971203516129784L;

	public TestBRCacheConfig(){

        this.nodesBufferSize = 1024*1024;
        this.nodesPageSize   = 1024;
        this.nodesSwapFactor = 0.3;
        this.indexBufferSize = 1024*1024;
        this.indexPageSize   = 1024;
        this.indexSwapFactor = 0.3;
        this.dataBufferSize  = 64*1024*1024;
        this.dataBlockSize   = 1024;
        this.dataPageSize    = 8*1024;
        this.dataSwapFactor  = 0.3;
        this.maxSizeEntry    = 1024*1024;
        this.maxSizeKey      = 100;
        this.swapperThread   = 4;
        this.dataPath        = "/mnt/brcache";
        this.memory          = new HeapMemory();
        this.entityFileManager = createEntityFileManager();
        
	}
	
	private EntityFileManager createEntityFileManager(){
		File path   = new File(this.dataPath);
		File txPath = new File(path, "tx");
		
		EntityFileManagerConfigurer efm           = new EntityFileManagerImp();
		LockProvider lp                           = new LockProviderImp();
		EntityFileTransactionManagerConfigurer tm = new EntityFileTransactionManagerImp();
		AsyncRecoveryTransactionLog rtl           = new AsyncRecoveryTransactionLog("recovery", txPath, tm);
		TransactionLog tl                         = new TransactionLogImp("binlog", txPath, tm);
		EntityFileTransactionFactory eftf         = new AsyncEntityFileTransactionFactory(rtl);
		
		rtl.setForceReload(true);
		
		tm.setTransactionLog(tl);
		tm.setRecoveryTransactionLog(rtl);
		tm.setEntityFileTransactionFactory(eftf);
		tm.setLockProvider(lp);
		tm.setTimeout(EntityFileTransactionManagerImp.DEFAULT_TIMEOUT);
		tm.setTransactionPath(txPath);
		tm.setEntityFileManagerConfigurer(efm);
		tm.setEnabledTransactionLog(false);
		
		efm.setEntityFileTransactionManager(tm);
		efm.setLockProvider(lp);
		efm.setPath(path);
		efm.init();
		
		return efm;
	}
	
}
