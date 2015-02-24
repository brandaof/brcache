/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache;

import com.brandao.uoutec.commons.collections.HugeArrayList;
import com.brandao.uoutec.commons.collections.StringTreeKey;
import com.brandao.uoutec.commons.collections.TreeHugeMap;
import com.brandao.uoutec.commons.collections.TreeKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author Cliente
 */
public class Cache implements Serializable{
    
    private final TreeHugeMap<TreeKey,DataMap> dataMap;

    private final HugeArrayList<byte[]> dataList;
    
    private final int segmentSize;
    
    private final BlockingQueue<Integer> freeSegments;

    private final int writeBufferLength;
    
    volatile long countRead;
    
    volatile long countWrite;
    
    volatile long countReadData;

    volatile long countWriteData;
    
    public Cache(){
        /*
        //cada item 8B
        double keyItens         = 13107200.0; // 100M
        double keySegments      = 1310.0/keyItens; //(10000 segmentos)
        double clearKeySegments = ((keyItens/keySegments)*0.3)/(keySegments*keyItens);

        // cada item 40B
        double nodeItens         = 2621440.0; //100MB
        double nodeSegments      = 262.0/nodeItens; //(10000 segmentos)
        double clearNodeSegments = ((nodeItens/nodeSegments)*0.3)/(nodeSegments*nodeItens);

        // cada item ?B
        double dataItens         = 100.0; //100MB
        double dataSegments      = 1.0/dataItens;//(1000 segmentos)
        double clearDataSegments = ((dataItens/dataSegments)*0.6)/(dataSegments*dataItens);
        */
        
        //cada item 8B
        double keyItens         = 32768000.0; // 2,5G
        double keySegments      = 3276.0/keyItens; //(10000 segmentos)
        double clearKeySegments = ((keyItens/keySegments)*0.3)/(keySegments*keyItens);

        // cada item 40B
        double nodeItens         = 6553600.0; //2,5G
        double nodeSegments      = 655.0/nodeItens; //(10000 segmentos)
        double clearNodeSegments = ((nodeItens/nodeSegments)*0.3)/(nodeSegments*nodeItens);

        // cada item ?B
        double dataItens         = 163840.0; //100MB
        double dataSegments      = 16.0/dataItens;//(1000 segmentos)
        double clearDataSegments = ((dataItens/dataSegments)*0.6)/(dataSegments*dataItens);
        
        this.dataMap =
                new TreeHugeMap<TreeKey, DataMap>(
                "/mnt2/var/webcache/dataMap",
                "data",
                (int)keyItens,
                clearKeySegments,
                keySegments,
                (int)nodeItens,
                clearNodeSegments,
                nodeSegments
                );
        
        this.dataList =
                new HugeArrayList<byte[]>(
                "/mnt2/var/webcache/dataList",
                "data",
                (int)dataItens,
                clearDataSegments,
                dataSegments
                );
        
        this.segmentSize = 16*1024;
        this.freeSegments = new LinkedBlockingQueue<Integer>();
        this.writeBufferLength = 1024*1024;
    }

    public void putObject(String key, long maxAliveTime, Object inputData) throws IOException{
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(bout);
        oout.writeObject(inputData);
        oout.flush();
        this.put(key, maxAliveTime, new ByteArrayInputStream(bout.toByteArray()));
    }

    public Object getObject(String key) throws IOException, ClassNotFoundException{
        InputStream in = this.get(key);
        if(in != null){
            ObjectInputStream oin = new ObjectInputStream(in);
            return oin.readObject();
        }
        else
            return null;
    }
    
    public void put(String key, long maxAliveTime, InputStream inputData) throws IOException{

        int[] segments = this.putData(inputData);
        this.putSegments(key, maxAliveTime, segments);
        
        countWrite++;
    }

    public InputStream get(String key){
        
        countRead++;
        
        DataMap map = this.dataMap.get(new StringTreeKey(key));
        
        if(map != null)
            return new CacheInputStream(this, map, this.dataList);
        else
            return null;
    }
    
    public boolean remove(String key){
        
        DataMap data = this.dataMap.get(new StringTreeKey(key));
        
        if(data != null){
            
            this.dataMap.put(new StringTreeKey(key), null);
            
            synchronized(this.dataList){
                int[] segments = data.getSegments();

                for(int segment: segments){
                    this.dataList.set(segment, null);
                    try{
                        this.freeSegments.put(segment);
                    }
                    catch(Throwable e){
                        throw new RuntimeException(e);
                    }
                }
            }
            return true;
        }
        else
            return false;
        
    }
    
    private void putSegments(String key, long maxAliveTime, int[] segmens){
        DataMap map = new DataMap();
        map.setMaxLiveTime(maxAliveTime);
        map.setSegments(segmens);
        this.dataMap.put(new StringTreeKey(key), map);
    }
    
    private int[] putData(InputStream inputData) throws IOException{
        List<Integer> segments = new ArrayList<Integer>();
        byte[] buffer = new byte[this.writeBufferLength];
        int read;
        
        while((read = inputData.read(buffer)) != -1){
           this.writeOnCache(buffer, 0, read, segments);
        }
        
        Integer[] segs = segments.toArray(new Integer[0]);
        int[] result = new int[segs.length];

        for(int i=0;i<segs.length;i++)
            result[i] = segs[i];

        return result;
    }

    private void writeOnCache(byte[] readBuf, int start, int offset, List<Integer> segments){
        
        byte[] writeBuf = new byte[this.segmentSize];
        int currentOffset = 0;
        
        int read = 0;
        int limitRead = offset - start;
        int maxWrite;
        int maxRead = 0;
        
        while(read < limitRead){
            
            maxWrite = writeBuf.length - currentOffset;
            maxRead  = limitRead - read;
            
            if(maxWrite > maxRead){
                System.arraycopy(readBuf, read, writeBuf, currentOffset, maxRead);
                currentOffset += maxRead;
                read += maxRead;
                this.countWriteData += maxRead;
            }
            else{
                System.arraycopy(readBuf, read, writeBuf, currentOffset, maxWrite);
                currentOffset += maxWrite;
                read += maxWrite;
                this.countWriteData += maxWrite;
                
                synchronized(this.dataList){
                    Integer segment = this.freeSegments.poll();
                    if(segment == null){
                        segment = this.dataList.size();
                        this.dataList.add(writeBuf);
                    }
                    else
                        this.dataList.set(segment, writeBuf);
                    
                    segments.add(segment);
                }

                writeBuf = new byte[this.segmentSize];
                currentOffset = 0;
            }
        }
        
        if(currentOffset != 0){
            synchronized(this.dataList){
                Integer segment = this.freeSegments.poll();
                if(segment == null){
                    segment = this.dataList.size();
                    this.dataList.add(Arrays.copyOf(writeBuf, maxRead));
                }
                else
                    this.dataList.set(segment, Arrays.copyOf(writeBuf, maxRead));

                segments.add(segment);
            }
        }
            
    }
    
    public long getCountRead(){
        return this.countRead;
    }

    public long getCountWrite(){
        return this.countWrite;
    }

    public long getCountReadData() {
        return countReadData;
    }
    
    public long getCountWriteData() {
        return countWriteData;
    }
    
    public long getReadPerSec(){
        try{
            long start = this.countRead;
            Thread.sleep(1000);
            return this.countRead - start;
        }
        catch(Exception e){
            return -1;
        }
    }
    
    public long getWritePerSec(){
        try{
            long start = this.countWrite;
            Thread.sleep(1000);
            return this.countWrite - start;
        }
        catch(Exception e){
            return -1;
        }
        
    }

    public long getReadDataPerSec(){
        try{
            long start = this.countReadData;
            Thread.sleep(1000);
            return this.countReadData - start;
        }
        catch(Exception e){
            return -1;
        }
    }
    
    public long getWriteDataPerSec(){
        try{
            long start = this.countWriteData;
            Thread.sleep(1000);
            return this.countWriteData - start;
        }
        catch(Exception e){
            return -1;
        }
        
    }
    
}
