package org.brandao.brcache.tx;

import org.brandao.brcache.BRCacheConfig;
import org.brandao.brcache.Configuration;
import org.brandao.brcache.SwaperStrategy;

public class BRCacheTransactionConfig extends BRCacheConfig{

	private static final long serialVersionUID = -4879757965527842004L;

	public void setConfiguration(Configuration config){
        this.nodesBufferSize     = config.getLong("transaction_nodes_buffer_size",		"3k");
        this.nodesPageSize       = config.getLong("transaction_nodes_page_size",		"1k");
        this.nodesSwapFactor     = config.getDouble("transaction_nodes_swap_factor",	"0.1");
        this.indexBufferSize     = config.getLong("transaction_index_buffer_size",		"1k");
        this.indexPageSize       = config.getLong("transaction_index_page_size",		"512b");
        this.indexSwapFactor     = config.getDouble("transaction_index_swap_factor",	"0.1");
        this.dataBufferSize      = config.getLong("transaction_data_buffer_size",		"6k");
        this.dataBlockSize       = config.getLong("transaction_data_block_size",		"512b");
        this.dataPageSize        = config.getLong("transaction_data_page_size",			"64b");
        this.dataSwapFactor      = config.getDouble("transaction_data_swap_factor",		"0.1");
        this.maxSizeEntry        = config.getLong("transaction_max_size_entry",			"1m");
        this.maxSizeKey          = config.getInt("transaction_max_size_key",			"48");
        this.swapperThread       = config.getInt("transaction_swapper_thread",			"1");
        this.dataPath            = config.getString("data_path",			"/var/brcache") + "/tx";
        this.swapper             = SwaperStrategy.valueOf(config.getString("transaction_swapper_type","file"));
    }
	
}
