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
            
        Configuration config = new Configuration();
        config.load(new FileInputStream(f));
     

        BrCacheServer server = new BrCacheServer(config);
        server.start();
    }
    
}
