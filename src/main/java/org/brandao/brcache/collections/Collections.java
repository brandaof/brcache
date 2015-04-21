/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.collections;

import java.io.File;
import java.net.InetAddress;
import java.util.Properties;

/**
 * Permite alterar as configurações comuns das coleções.
 * 
 * @author Brandao
 */
public class Collections {

    public static final String DEFAULT_TMP_DIR = System.getProperty("java.io.tmpdir");
    
    private static final Properties configuration = new Properties();
    
    private static boolean initialized = false;
    
    private static File path;
    
    private static String defaultPath = DEFAULT_TMP_DIR + "/collections";
    
    private static File defaultFilePath = new File(defaultPath);
    
    private static int defaultSegmentSize = 120;
    
    private volatile static long collectionID = 0;

    private static String serverId;
    
    private static String startTime;
    
    /**
     * Obtém as configuração comuns às todas as coleções.
     * @return Configuração.
     */
    public static Properties getConfiguration(){
        return configuration;
    }
    
    /**
     * Gera uma identificação única usada para identificar uma coleção.
     * @return Identificação.
     */
    public static String getNextId() {

        if (!initialized)
            initialize();

        return 
            new String(
                serverId +
                "#" +
                startTime + 
                "#" +
                "BRC" +
                "#" +
                Long.toString(collectionID++, 36)
            );
    }

    /**
     * Defie a pasta raiz onde serão persistidas as entidades de uma coleção.
     * @param aPath Pasta.
     */
    public static void setPath(String aPath) {
        path = new File(aPath);
        if (!path.exists()) {
            path.mkdirs();
        }
    }

    /**
     * Obtém a pasta raiz onde serão persistidas as entidades de uma coleção.
     * @return Pasta.
     */
    public static File getPath() {
        return path == null ? defaultFilePath : path;
    }

    private static synchronized void initialize() {
        if (initialized)
            return;

        initialized = true;
        
        try{
            InetAddress addr = InetAddress.getLocalHost();	
            serverId = addr.getHostAddress();
            startTime = Long.toString(System.currentTimeMillis(), 36);
        }
        catch(Exception e){
            serverId = "localhost";
        }
        
        deleteDir(defaultFilePath);
    }

    static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }
}
