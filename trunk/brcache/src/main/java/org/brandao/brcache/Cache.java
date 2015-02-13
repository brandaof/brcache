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

/**
 *
 * @author Cliente
 */
public class Cache implements Serializable{
    
    private final TreeHugeMap<StringTreeKey,DataMap> dataMap;
    
    private final HugeArrayList<byte[]> dataList;
    
    private final int segmentSize;
    
    private int maxdataOnMemory;
    
    public Cache(){
        /*
        this.dataMap =
                new TreeHugeMap<StringTreeKey, DataMap>(
                "/mnt2/var/webcache/dataMap",
                "data",
                10000,
                0.001F,
                0.001F,
                10000,
                0.01F,
                0.01F);

        this.dataList =
                new HugeArrayList<byte[]>(
                "/mnt2/var/webcache/dataList",
                "data",
                10000,
                0.01F,
                0.01F);
        */
        
        this.dataMap =
                new TreeHugeMap<StringTreeKey, DataMap>(
                "/mnt2/var/webcache/dataMap",
                "data",
                600000, //Quantidade de nós na memória
                0.0005F,//Fator de limpeza de segmentos
                0.0005F,//Fator de agrupamentos dos nós
                300000, //Quantidade de itens em memória
                0.0001F,//Fator de limpeza de segmentos
                0.0001F //Fator de agrupamentos dos itens
                );

        this.dataList =
                new HugeArrayList<byte[]>(
                "/mnt2/var/webcache/dataList",
                "data",
                200000, //Quantidade de itens em memória
                0.0001F,//Fator de limpeza de segmentos
                0.0001F //Fator de agrupamentos dos itens
                );
        
        this.segmentSize = 6*1024;
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
        int[] segments = this.putData(inputData);
        this.putSegments(key, maxAliveTime, segments);
    }

    public InputStream get(String key){
        DataMap map = this.dataMap.get(new StringTreeKey(key));
        
        if(map != null)
            return new CacheInputStream(map, this.dataList);
        else
            return null;
    }
    
    public synchronized void remove(String key){
        DataMap data = this.dataMap.get(new StringTreeKey(key));
        if(data != null){
            
            this.dataMap.remove(new StringTreeKey(key));
            
            int[] segments = data.getSegments();
            for(int segment: segments){
                this.dataList.remove(segment);
            }
            
        }
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

                this.dataList.add(tmp);
                int segment = this.dataList.size() - 1;

                if(segment % 10000 == 0){
                    System.out.println(segment);
                }

                segments.add(segment);
            }

        }

        Integer[] segs = segments.toArray(new Integer[0]);
        int[] result = new int[segs.length];

        for(int i=0;i<segs.length;i++)
            result[i] = segs[i];

        return result;
    }
    
}
