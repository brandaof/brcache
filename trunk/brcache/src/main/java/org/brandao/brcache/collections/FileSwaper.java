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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 *
 * @author Brandao
 */
public class FileSwaper<T> implements Swapper<T> {
    
    private String id;
    
    private String pathName;
    
    private transient File path;
    
    private transient RandomAccessFile dataFile;
    
    private volatile transient boolean hasCreatePath;
    
    private long segmentSize;
    
    public FileSwaper(String id, String path, int segmentSize){
        this.id = id;
        this.pathName = path;
        this.segmentSize = segmentSize;
    }
    
    public synchronized void persistDiskItem(Integer index, Entry<T> item) {
        try {
            if (!hasCreatePath)
                createPath();

            long pos = index*this.segmentSize;
            this.dataFile.seek(pos);
            ObjectOutputStream oOut = null;
            try {
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                
                oOut = new ObjectOutputStream(bout);
                oOut.writeObject(item.getItem());
                oOut.flush();
                
                byte[] data = bout.toByteArray();
                if(data.length > this.segmentSize)
                    throw new IllegalStateException(data.length + " > " + this.segmentSize);
                else
                    data = Arrays.copyOf(data, (int)this.segmentSize);
                
                this.dataFile.write(data);
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
    public synchronized Entry<T> readDiskItem(Integer index) {
        long actual;
        long pos;
        long length;
        
        try {
            if (!hasCreatePath)
                createPath();
            
            actual = this.dataFile.getFilePointer();
            pos = index*this.segmentSize;
            length = this.dataFile.length();
            this.dataFile.seek(pos);
            
            ObjectInputStream iIn = null;
            try {
                byte[] data = new byte[(int)this.segmentSize];
                this.dataFile.readFully(data);
                
                ByteArrayInputStream bin = new ByteArrayInputStream(data);
                iIn = new ObjectInputStream(bin);
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
    
    private synchronized void createPath() throws FileNotFoundException, IOException {

        if (this.hasCreatePath) {
            return;
        }

        File root = this.pathName == null? Collections.getPath() : new File(this.pathName);

        path = new File(root, id);

        if (!path.exists())
            path.mkdirs();

        File fileDto = new File(this.path, this.id + "-data.swp");
        fileDto.createNewFile();
        
        this.dataFile = new RandomAccessFile(fileDto, "rw");
        this.dataFile.seek(0);
        this.hasCreatePath = true;
    }
    
    public void setPath(String value) {
        this.pathName = value;
    }

    public void setId(String value) {
        this.id = value;
    }
    
    
}
