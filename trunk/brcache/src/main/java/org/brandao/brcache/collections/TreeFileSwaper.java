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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * As entidades são enviadas para o disco e armazenadas 
 * em uma árvore de diretórios.
 * 
 * @author Brandao
 */
public class TreeFileSwaper implements DiskSwapper {
    
    public static final String PATH = "brcache.swapper.path";
    
    private String id;
    
    private String pathName;
    
    private transient File path;
    
    private transient boolean hasCreatePath;

    public TreeFileSwaper(){
        this.pathName = Collections.getConfiguration().getProperty(PATH);
    }
    
    public void sendItem(Integer index, Entry<?> item) {
        try {
            if (!hasCreatePath)
                createPath();

            String dataPathName = getPath(String.valueOf(index));
            File dataPath = new File(path, dataPathName);
            
            synchronized(this){
                if(!dataPath.exists())
                    dataPath.mkdirs();
            }
            
            File swp = new File(dataPath, index + ".swp");

            if (!swp.exists())
                swp.createNewFile();

            OutputStream out;
            ObjectOutputStream oOut = null;
            try {
                out = new FileOutputStream(swp);
                oOut = new ObjectOutputStream(out);
                oOut.writeObject(item.getItem());
                oOut.flush();
            }
            finally {
                if (oOut != null) {
                    oOut.flush();
                    oOut.close();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public Entry<?> getItem(Integer index) {
        try {
            if (!hasCreatePath)
                createPath();
            
            String dataPathName = getPath(String.valueOf(index));
            File dataPath = new File(path, dataPathName);
            File swp = new File(dataPath, index + ".swp");

            if (!swp.exists())
                return null;
            
            InputStream in;
            ObjectInputStream iIn = null;
            try {
                in = new FileInputStream(swp);
                iIn = new ObjectInputStream(in);
                Object item = iIn.readObject();

                Entry<?> entry = new Entry<Object>(index, false, item);
                entry.setNeedReload(false);
                return entry;
            }
            finally {
                if(iIn != null)
                    iIn.close();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    private synchronized void createPath() {

        if (this.hasCreatePath) {
            return;
        }

        File root = this.pathName == null? Collections.getPath() : new File(this.pathName);

        path = new File(root, id);

        if (!path.exists()) {
            path.mkdirs();
        }

        this.hasCreatePath = true;
    }
    
    private String getPath(String str){
        int length = (str.length()/4) > 1? str.length()/4 : 3;

        String[] pathParts = new String[str.length()/length];

        int index = 0;
        for(int i=0;i<pathParts.length;i++){
                int start = index;
                int end = index + length;

                pathParts[i] = str.substring(start, end<str.length()? end : str.length()); 
                index = index + length;
        }
        
        StringBuilder persistedImagePathBuilder = new StringBuilder("");

        for(String path: pathParts)
                persistedImagePathBuilder.append("/").append(path);
        
        return persistedImagePathBuilder.toString();
    }

    public void setPath(String value) {
        this.pathName = value;
    }

    public void setId(String value) {
        this.id = value;
    }

    public void clear() {
        File root = this.pathName == null? Collections.getPath() : new File(this.pathName);
        File rootPath = new File(root, id);
        if(rootPath.exists())
            Collections.deleteDir(rootPath);
    }

    public void setRootPath(String path) {
        this.pathName = path;
    }

    public String getRootPath() {
        return this.pathName;
    }
    
    
}
