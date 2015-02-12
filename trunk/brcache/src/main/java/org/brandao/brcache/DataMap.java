/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache;

import java.io.Serializable;

/**
 *
 * @author Cliente
 */
public class DataMap implements Serializable{
    
    private String id;
    
    private long maxLiveTime;
    
    private int[] segments;

    public long getMaxLiveTime() {
        return maxLiveTime;
    }

    public void setMaxLiveTime(long maxLiveTime) {
        this.maxLiveTime = maxLiveTime;
    }

    public int[] getSegments() {
        return segments;
    }

    public void setSegments(int[] segments) {
        this.segments = segments;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }
    
}
