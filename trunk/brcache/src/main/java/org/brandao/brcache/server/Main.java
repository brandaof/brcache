/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.brandao.brcache.Cache;

/**
 *
 * @author Cliente
 */
public class Main {
    
    public static void main(String[] params) throws IOException{
        File f = new File("brcache.conf");
        
        if(!f.exists() || !f.canRead()){
            System.out.println("configuration not found!");
            System.exit(2);
        }
            
        Properties config = new Properties();
        config.load(new FileInputStream(f));
     

        double port               = getNumber(config.getProperty("port","1044"));
        double max_connections    = getNumber(config.getProperty("max_connections","10"));
        double timeout_connection = getNumber(config.getProperty("timeout_connection","0"));
        boolean reuse_address     = config.getProperty("reuse_address","false").equalsIgnoreCase("true");
        double nodes_size         = getNumber(config.getProperty("nodes_size","32768000"));
        double nodes_swap_size    = getNumber(config.getProperty("nodes_swap_size","3276"));
        double nodes_swap_factor  = getNumber(config.getProperty("nodes_swap_factor","0.3"));
        double index_size         = getNumber(config.getProperty("index_size","6553600"));
        double index_swap_size    = getNumber(config.getProperty("index_swap_size","655"));
        double index_swap_factor  = getNumber(config.getProperty("index_swap_factor","0.3"));
        double data_size          = getNumber(config.getProperty("data_size","104857600"));
        double data_swap_size     = getNumber(config.getProperty("data_swap_size","1048576"));
        double data_swap_factor   = getNumber(config.getProperty("data_swap_factor","0.6"));
        String data_path          = config.getProperty("data_path","/var/brcache");
        double max_slab_size      = getNumber(config.getProperty("max_slab_size","16384"));
        double write_buffer_size  = getNumber(config.getProperty("write_buffer_size","1048576"));
        double max_size_entry     = getNumber(config.getProperty("max_size_entry","10485760"));
        double max_size_key       = getNumber(config.getProperty("max_size_key","48"));
        
        
        double nodesOnMemory          = nodes_size/8.0;
        double nodesPerSegment        = nodes_swap_size/8.0;
        double swapSegmentNodesFactor = nodes_swap_factor;
        
        double indexOnMemory          = index_size/40.0;
        double indexPerSegment        = index_swap_size/40.0;
        double swapSegmentIndexFactor = index_swap_factor;
        
        double bytesOnMemory          = data_size/max_slab_size;
        double bytesPerSegment        = data_swap_size/max_slab_size;
        double swapSegmentsFactor     = data_swap_factor;
        
        String path                   = data_path;
        int maxBytesStoragePerGroup   = (int)max_slab_size;
        int writeBufferSize           = (int)write_buffer_size;
        int maxBytesToStorageEntry    = (int)max_size_entry;
        int maxLengthKey              = (int)max_size_key;
        
        Cache cache = new Cache(
            nodesOnMemory,
            nodesPerSegment,
            swapSegmentNodesFactor,
            indexOnMemory,
            indexPerSegment,
            swapSegmentIndexFactor,
            bytesOnMemory,
            bytesPerSegment,
            swapSegmentsFactor,
            path,
            (int)maxBytesStoragePerGroup,
            (int)writeBufferSize,
            (int)maxBytesToStorageEntry,
            (int)maxLengthKey);
        
        BrCacheServer server = 
                new BrCacheServer(
                        (int)port, 
                        0, 
                        (int)max_connections, 
                        (int)timeout_connection, 
                        reuse_address, 
                        cache);
        
        server.start();
    }
    
    private static double getNumber(String value){
        
        if(value.matches("^\\d+(\\.\\d+){0,1}$"))
            return Double.parseDouble(value);
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(B|b)$"))
            return Double.parseDouble(value.substring(0,value.length()-1));
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(K|k)$"))
            return Double.parseDouble(value.substring(0,value.length()-1))*1024;
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(M|m)$"))
            return Double.parseDouble(value.substring(0,value.length()-1))*1024*1024;
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(G|g)$"))
            return Double.parseDouble(value.substring(0,value.length()-1))*1024*1024*1024;
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(T|t)$"))
            return Double.parseDouble(value.substring(0,value.length()-1))*1024*1024*1024*1024;
        else
            throw new NumberFormatException();
        
    }
}
