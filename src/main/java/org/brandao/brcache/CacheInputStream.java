/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache;

import com.brandao.uoutec.commons.collections.HugeArrayList;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Cliente
 */
public class CacheInputStream extends InputStream{

    private DataMap map;
    
    private HugeArrayList<byte[]> dataList;
    
    private int currentSegmentIndex;
    
    private int currentDataindex;
    
    public CacheInputStream(DataMap map, HugeArrayList<byte[]> dataList){
        this.map = map;
        this.dataList = dataList;
        this.currentDataindex = 0;
        this.currentSegmentIndex = 0;
    }
    
    @Override
    public int read(byte[] bytes, int i, int i1) throws IOException {
        return transfer(bytes, i, i1 );
    }
    
    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int l = transfer(b, 0, 1);
        
        if(l == -1)
            return -1;
        else
            return b[0];
    }
    
    private int transfer(byte[] dest, int destPos, int length ){
        int[] segments = this.map.getSegments();
        
        if(this.currentSegmentIndex >= segments.length)
            return -1;
        
        byte[] origin  = this.dataList.get(segments[this.currentSegmentIndex]);
        
        int read = 0;
        
        while(length > 0 && origin != null){
            
            if(length > origin.length - this.currentDataindex){
                
                int lenRead = origin.length - this.currentDataindex;
                
                System.arraycopy(origin, this.currentDataindex, dest, destPos, lenRead);
                
                length -= lenRead;
                read += lenRead;
                destPos += lenRead;
                this.currentDataindex = 0;
                this.currentSegmentIndex++;
                origin = 
                    this.currentSegmentIndex < segments.length? 
                        this.dataList.get(segments[this.currentSegmentIndex]) :
                        null;
            }
            else{
                int lenRead = length;
                System.arraycopy(origin, this.currentDataindex, dest, destPos, lenRead);
                
                destPos += lenRead;
                read += lenRead;
                length -= lenRead;
                this.currentDataindex += lenRead;
            }
        }
        
        return read;
    }
    
}
