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

}
