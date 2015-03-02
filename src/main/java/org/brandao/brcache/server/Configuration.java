/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.math.BigDecimal;
import java.util.Properties;

/**
 *
 * @author Cliente
 */
public class Configuration extends Properties{

    private static final BigDecimal KB = new BigDecimal(1024);

    private static final BigDecimal MB = new BigDecimal(1024*1024);

    private static final BigDecimal GB = new BigDecimal(1024*1024*1024);

    private static final BigDecimal TB = new BigDecimal(1024*1024*1024*1024);
    
    public boolean getBoolean(String property, String defaultValue){
        String value = super.getProperty(property, defaultValue);
        
        if(value == null)
            throw new IllegalStateException("property not found: " + property);
        else
            return value.equalsIgnoreCase("true");
    }
    
    public String getString(String property, String defaultValue){
        String value = super.getProperty(property, defaultValue);
        
        if(value == null)
            throw new IllegalStateException("property not found: " + property);
        else
            return value;
    }
    
    public int getInt(String property, String defaultValue){
        return this.getNumber(property, defaultValue).intValue();
    }

    public long getLong(String property, String defaultValue){
        return this.getNumber(property, defaultValue).longValue();
    }
    
    public double getDouble(String property, String defaultValue){
        return this.getNumber(property, defaultValue).doubleValue();
    }

    public float getFloat(String property, String defaultValue){
        return this.getNumber(property, defaultValue).floatValue();
    }

    public BigDecimal getNumber(String property, String defaultValue){
        String value = super.getProperty(property, defaultValue);
        
        if(value == null)
            throw new IllegalStateException("property not found: " + property);
        
        if(value.matches("^\\d+(\\.\\d+){0,1}$"))
            return new BigDecimal(value);
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(B|b)$"))
            return new BigDecimal(value.substring(0,value.length()-1));
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(K|k)$"))
            return new BigDecimal(value.substring(0,value.length()-1)).multiply(KB);
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(M|m)$"))
            return new BigDecimal(value.substring(0,value.length()-1)).multiply(MB);
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(G|g)$"))
            return new BigDecimal(value.substring(0,value.length()-1)).multiply(GB);
        else
        if(value.matches("^\\d+(\\.\\d+){0,1}(T|t)$"))
            return new BigDecimal(value.substring(0,value.length()-1)).multiply(TB);
        else
            throw new NumberFormatException();
        
    }
    
}
