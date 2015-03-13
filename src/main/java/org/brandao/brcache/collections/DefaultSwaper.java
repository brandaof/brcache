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
 *
 * @author Brandao
 */
public class DefaultSwaper<T> implements Swaper<T> {
    
    private String id;
    
    private String pathName;
    
    private transient File path;
    
    private transient boolean hasCreatePath;
    
    public DefaultSwaper(String id, String path){
        this.id = id;
        this.pathName = path;
    }
    
    public void persistDiskItem(Integer index, Entry<T> item) {
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

    @SuppressWarnings({"unchecked"})
    public Entry<T> readDiskItem(Integer index) {
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
                T item = (T) iIn.readObject();

                Entry<T> entry = new Entry<T>(index, false, item);
                entry.setOnDisk(false);
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
    
    
}
