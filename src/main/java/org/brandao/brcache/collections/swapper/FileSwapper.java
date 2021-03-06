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

package org.brandao.brcache.collections.swapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.brandao.brcache.collections.Collections;
import org.brandao.brcache.collections.DiskSwapper;
import org.brandao.brcache.collections.Entry;
import org.brandao.brcache.collections.Swapper;
import org.brandao.brcache.collections.fileswapper.DataBlockEntityFile;
import org.brandao.brcache.collections.fileswapper.DataBlockInputStream;
import org.brandao.brcache.collections.fileswapper.DataBlockOutputStream;
import org.brandao.brcache.collections.fileswapper.DataChain;
import org.brandao.brcache.collections.fileswapper.SimpleIndex;
import org.brandao.brcache.collections.fileswapper.SimpleIndexEntityFile;

/**
 * As entidades são enviadas para o disco e armazenadas 
 * em um único arquivo.
 * 
 * @author Brandao
 */
public class FileSwapper implements DiskSwapper {
    
	private static final long serialVersionUID = -9192895825998099336L;

	private String id;
    
    private String pathName;
    
    private File path;
    
    private SimpleIndex index;
    
    private SimpleIndexEntityFile indexFile;

    private DataBlockEntityFile dataFile;
    
    private volatile boolean hasCreatePath;
    
    public FileSwapper(){
        this.index = new SimpleIndex();
    }
    
    public synchronized void sendItem(long index, Entry<?> item) {
        try {
            ObjectOutputStream oOut = null;
            DataBlockOutputStream bout = null;
            try {
                bout = new DataBlockOutputStream(this.dataFile.getBlockSize());
                
                oOut = new ObjectOutputStream(bout);
                oOut.writeObject(item.getItem());
                oOut.flush();
            }
            finally {
                if (oOut != null) {
                    oOut.flush();
                    oOut.close();
                }
            }
            
            String idx = Long.toString(index, Character.MAX_RADIX);
            long reference = this.index.get(idx, this.indexFile);
            	
            if(reference == -1){
                reference = DataChain.save(bout.getBlocks(), this.dataFile);
                this.index.registry(idx, reference, this.indexFile);
            }
            else{
                reference = DataChain.update(reference, bout.getBlocks(), this.dataFile);
                this.index.registry(idx, reference, this.indexFile);
            }
            this.dataFile.flush();
            this.indexFile.flush();
        } 
        catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    public synchronized Entry<?> getItem(long index) {
        try {

            String idx = Long.toString(index, Character.MAX_RADIX);
            long reference = this.index.get(idx, this.indexFile);

            if(reference == -1)
            	return null;
            //	throw new IllegalStateException(String.valueOf(index));
            
            ObjectInputStream iIn = null;
            try {
                iIn = new ObjectInputStream(new DataBlockInputStream(reference, this.dataFile));
                Object item = iIn.readObject();

                Entry<?> entry = new Entry<Object>(index, false, item);
                entry.setNeedReload(false);
                return entry;
            }
            finally {
                if(iIn != null)
                    iIn.close();
            }
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }
    
    private synchronized void createPath() throws FileNotFoundException, IOException {

        if (this.hasCreatePath) {
            return;
        }

        this.path = this.pathName == null? Collections.getPath() : new File(this.pathName);

        if (!this.path.exists())
            this.path.mkdirs();

        File idxFile = new File(this.path, this.id + ".idx");
        this.indexFile = new SimpleIndexEntityFile(idxFile);
        this.indexFile.createNewFile();

        File datFile = new File(this.path, this.id + ".dat");
        this.dataFile = new DataBlockEntityFile(datFile, 2*1024);
        this.dataFile.createNewFile();
        
        this.hasCreatePath = true;
    }
    
    public void setRootPath(String value) {
        this.pathName = value;
    }

    public String getRootPath(){
    	return this.pathName;
    }
    
    public void setId(String value) {
        try{
            this.id = value;
            this.createPath();
        }
        catch(Throwable e){
            throw new IllegalArgumentException(value, e);
        }
    }

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
    	stream.writeUTF(id == null? "" : id);
    	stream.writeUTF(pathName == null? "" : pathName);
    	stream.writeBoolean(this.hasCreatePath);
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
    	
    	this.id            = stream.readUTF();
    	this.pathName      = stream.readUTF();
        this.hasCreatePath = stream.readBoolean();

        this.id       = this.id.isEmpty()? null : this.id;
        this.pathName = this.pathName.isEmpty()? null : this.pathName;
        
        if(!this.hasCreatePath){
        	return;
        }
        
        this.path = this.pathName == null? Collections.getPath() : new File(this.pathName);

        if (!this.path.exists()){
            this.path.mkdirs();
        }

        File idxFile = new File(this.path, this.id + ".idx");
        this.indexFile = new SimpleIndexEntityFile(idxFile);
        this.indexFile.open();

        File datFile = new File(this.path, this.id + ".dat");
        this.dataFile = new DataBlockEntityFile(datFile, 2*1024);
        this.dataFile.open();
        
        this.hasCreatePath = true;
    }
    
    public void clear() {
    	try{
    		this.indexFile.createNewFile();
    		this.dataFile.createNewFile();
    		/*
	        File rootPath  = this.pathName == null? Collections.getPath() : new File(this.pathName);
	        if(rootPath.exists())
	            Collections.deleteDir(rootPath);
            */
    	}
    	catch(Throwable e){
    		throw new IllegalStateException(e);
    	}
    }

	public void destroy() {
    	try{
    		this.indexFile.close();
    		this.indexFile.delete();
    		this.dataFile.close();
    		this.dataFile.delete();
    		this.hasCreatePath = false;
    	}
    	catch(Throwable e){
    		throw new IllegalStateException(e);
    	}
	}
    
	public Swapper clone(){
		try{
			return (Swapper) super.clone();
		}
		catch(Throwable e){
			return null;
		}
	}

}
