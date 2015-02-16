/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache;

import com.brandao.uoutec.commons.collections.HugeArrayList;
import com.brandao.uoutec.commons.collections.StringTreeKey;
import com.brandao.uoutec.commons.collections.TreeHugeMap;
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
    
    private final TreeHugeMap<StringTreeKey,DataMap> dataMap;
    
    private final HugeArrayList<byte[]> dataList;
    
    private final int segmentSize;
    
    private final BlockingQueue<Integer> freeSegments;
    
    private int maxdataOnMemory;
    
    private long writePerSec;
    
    private long readPerSec;
    
    private long countRead;
    
    private long countWrite;
    
    private long lastCheckRead;

    private long lastCheckWrite;
    
    private final Object lockReadCount = new Object();
    
    public Cache(){
        /*
        this.dataMap =
                new TreeHugeMap<StringTreeKey, DataMap>(
                "/mnt2/var/webcache/dataMap",
                "data",
                10000,
                0.001F,
                0.01F,
                10000,
                0.001F,
                0.01F);

        this.dataList =
                new HugeArrayList<byte[]>(
                "/mnt2/var/webcache/dataList",
                "data",
                10000,
                0.001F,
                0.01F);
        */

        this.lastCheckWrite = System.currentTimeMillis();
        this.lastCheckRead  = System.currentTimeMillis();
        
        double keyItens         = 600000.0;
        double keySegments      = 5.0/keyItens;
        double clearKeySegments = ((keyItens/5.0)*0.6)/(keySegments*keyItens);

        double nodeItens         = 300000.0;
        double nodeSegments      = 5.0/nodeItens;
        double clearNodeSegments = ((nodeItens/5.0)*0.6)/(nodeSegments*nodeItens);

        double dataItens         = 200000.0;
        double dataSegments      = 5.0/dataItens;
        double clearDataSegments = ((dataItens/5.0)*0.6)/(dataSegments*dataItens);
        
        this.dataMap =
                new TreeHugeMap<StringTreeKey, DataMap>(
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
        
        /*
        this.dataMap =
                new TreeHugeMap<StringTreeKey, DataMap>(
                "/mnt2/var/webcache/dataMap",
                "data",
                600000, //Quantidade de nós na memória
                0.01F,//Fator de limpeza de segmentos
                0.00005F,//Fator de agrupamentos dos nós
                300000, //Quantidade de itens em memória
                0.01F,//Fator de limpeza de segmentos
                0.0001F //Fator de agrupamentos dos itens
                );

        this.dataList =
                new HugeArrayList<byte[]>(
                "/mnt2/var/webcache/dataList",
                "data",
                200000, //Quantidade de itens em memória
                0.01F,//Fator de limpeza de segmentos
                0.00005F //Fator de agrupamentos dos itens
                );
        */
        
        this.segmentSize = 6*1024;
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
    
    public synchronized void put(String key, long maxAliveTime, InputStream inputData) throws IOException{

        long currentTime = System.currentTimeMillis();
        if(currentTime - this.lastCheckWrite >= 1000 ){
            this.lastCheckWrite = currentTime;
            this.writePerSec = this.countWrite;
            this.countWrite = 0;
        }
        else
            countWrite++;
        
        int[] segments = this.putData(inputData);
        this.putSegments(key, maxAliveTime, segments);
    }

    public InputStream get(String key){
        
        synchronized(lockReadCount){
            long currentTime = System.currentTimeMillis();

            if(currentTime - this.lastCheckRead >= 1000 ){
                this.lastCheckRead = currentTime;
                this.readPerSec = this.countRead;
                this.countRead = 0;
            }
            else
                countRead++;
        }
        
        DataMap map = this.dataMap.get(new StringTreeKey(key));
        
        if(map != null)
            return new CacheInputStream(map, this.dataList);
        else
            return null;
    }
    
    public synchronized boolean remove(String key){
        
        DataMap data = this.dataMap.get(new StringTreeKey(key));
        
        if(data != null){
            
            this.dataMap.put(new StringTreeKey(key), null);
            
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

                Integer segment = this.freeSegments.poll();
                
                if(segment == null){
                    this.dataList.add(tmp);
                    segment = this.dataList.size() - 1;
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

    public long getWritePerSec() {
        return writePerSec;
    }

    public long getReadPerSec() {
        return readPerSec;
    }
    
}
