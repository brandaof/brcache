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
     

        double port                    = getNumber(config.getProperty("port","1044"));
        double minConnections          = getNumber(config.getProperty("min_connections","10"));
        double maxConnections          = getNumber(config.getProperty("max_connections","1024"));
        double timeoutConnection       = getNumber(config.getProperty("timeout_connection","0"));
        boolean reuseAddress           = config.getProperty("reuse_address","false").equalsIgnoreCase("true");
        double nodesOnMemory           = getNumber(config.getProperty("nodes_on_memory","32768000"));
        double nodesPerSegment         = getNumber(config.getProperty("nodes_per_segment","3276"));
        double swapSegmentNodesFactor  = getNumber(config.getProperty("swap_segment_nodes_factor","0.3"));
        double indexOnMemory           = getNumber(config.getProperty("index_on_memory","6553600"));
        double indexPerSegment         = getNumber(config.getProperty("index_per_segment","655"));
        double swapSegmentIndexFactor  = getNumber(config.getProperty("swap_segment_index_factor","0.3"));
        double bytesOnMemory           = getNumber(config.getProperty("bytes_on_memory","104857600"));
        double bytesPerSegment         = getNumber(config.getProperty("bytes_per_segment","1048576"));
        double swapSegmentsFactor      = getNumber(config.getProperty("swap_segments_factor","0.6"));
        String dataPath                = config.getProperty("data_path","/var/brcache");
        double maxBytesStoragePerGroup = getNumber(config.getProperty("max_bytes_storage_per_group","16384"));
        double writeBufferSize         = getNumber(config.getProperty("write_buffer_size","1048576"));
        double maxBytesToStorageEntry  = getNumber(config.getProperty("max_bytes_to_storage_entry","10485760"));
        double maxLengthKey     = getNumber(config.getProperty("max_length_Key","48"));
        
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
            dataPath,
            (int)maxBytesStoragePerGroup,
            (int)writeBufferSize,
            (int)maxBytesToStorageEntry,
            (int)maxLengthKey);
        
        BrCacheServer server = 
                new BrCacheServer(
                        (int)port, 
                        (int)minConnections, 
                        (int)maxConnections, 
                        (int)timeoutConnection, 
                        reuseAddress, 
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
