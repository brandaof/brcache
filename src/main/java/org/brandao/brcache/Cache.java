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
    
    volatile long countRead;
    
    volatile long countWrite;
    
    volatile long countReadData;

    volatile long countWriteData;
    
    public Cache(){
        //cada item 8B
        double keyItens         = 1310720.0; // 10M
        double keySegments      = 131.0/keyItens; //(10000 segmentos)
        double clearKeySegments = ((keyItens/keySegments)*0.4)/(keySegments*keyItens);

        // cada item 40B
        double nodeItens         = 262144.0; //10MB
        double nodeSegments      = 26.0/nodeItens; //(10000 segmentos)
        double clearNodeSegments = ((nodeItens/nodeSegments)*0.4)/(nodeSegments*nodeItens);

        // cada item ?B
        double dataItens         = 51200.0; //100MB
        double dataSegments      = 51.0/dataItens;//(1000 segmentos)
        double clearDataSegments = ((dataItens/dataSegments)*0.3)/(dataSegments*dataItens);

        this.dataMap =
                new TreeHugeMap<TreeKey, DataMap>(
                "/mnt2/var/webcache/dataMap",
                "data",
                (int)(keyItens + keyItens*0.6),
                clearKeySegments,
                keySegments,
                (int)(nodeItens + nodeItens*0.6),
                clearNodeSegments,
                nodeSegments
                );
        
        this.dataList =
                new HugeArrayList<byte[]>(
                "/mnt2/var/webcache/dataList",
                "data",
                (int)(dataItens + dataItens*0.6),
                clearDataSegments,
                dataSegments
                );
        
        this.segmentSize = 2*1024;
        this.freeSegments = new LinkedBlockingQueue<Integer>();
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
        //int[] segments = new int[]{0,0,0,0};
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
        byte[] buffer = new byte[this.segmentSize];
        int length = 0;

        while(length > -1){

            length = inputData.read(buffer, 0, buffer.length);

            if(length > 0){
                byte[] tmp = new byte[length];
                System.arraycopy(buffer, 0, tmp, 0, length);

                Integer segment;
                    
                    segment = this.freeSegments.poll();

                    if(segment == null){
                        this.countWriteData += length;
                        synchronized(this.dataList){
                            this.dataList.add(tmp);
                            segment = this.dataList.size() - 1;
                        }
                    }
                    else
                        this.dataList.set(segment, tmp);
                    

                segments.add(segment);
            }

        }

        Integer[] segs = segments.toArray(new Integer[0]);
        int[] result = new int[segs.length];

        for(int i=0;i<segs.length;i++)
            result[i] = segs[i];

        return result;
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
