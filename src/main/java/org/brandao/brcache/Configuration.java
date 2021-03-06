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

package org.brandao.brcache;

import java.math.BigDecimal;
import java.util.Properties;

/**
 * Propriedades do cache.
 * 
 * @author Brandao
 */
public class Configuration extends Properties{

	private static final long serialVersionUID = -8191796505743584525L;

	private static final BigDecimal KB = new BigDecimal(1024);

    private static final BigDecimal MB = new BigDecimal(1024*1024);

    private static final BigDecimal GB = new BigDecimal(1024*1024*1024);

    private static final BigDecimal TB = new BigDecimal(1024*1024*1024*1024);

    /**
     * Obtém a propriedade do tipo <code>Object</code>.
     * @param property propriedade.
     * @param defaultValue valor padrão.
     * @return propriedade.
     */
    public Object getObject(String property, String defaultValue){
        String value = super.getProperty(property, defaultValue);
        
        if(value == null)
            throw new IllegalStateException("property not found: " + property);
        else{
            try{
            	Class<?> clazz = Class.forName(value, true, Thread.currentThread().getContextClassLoader());
            	return clazz.newInstance();
            }
            catch(Throwable e){
                throw new IllegalStateException("invalid property: " + property);
            }
        }
    }
    
    /**
     * Obtém uma propriedade do tipo <code>boolean</code>.
     * @param property propriedade.
     * @param defaultValue valor padrão.
     * @return propriedade.
     */
    public boolean getBoolean(String property, String defaultValue){
        String value = super.getProperty(property, defaultValue);
        
        if(value == null)
            throw new IllegalStateException("property not found: " + property);
        else
            return value.equalsIgnoreCase("true");
    }
    
    /**
     * Obtém a propriedade do tipo <code>String</code>.
     * @param property propriedade.
     * @param defaultValue valor padrão.
     * @return propriedade.
     */
    public String getString(String property, String defaultValue){
        String value = super.getProperty(property, defaultValue);
        
        if(value == null)
            throw new IllegalStateException("property not found: " + property);
        else
            return value;
    }
    
    /**
     * Obtém a propriedade do tipo <code>int</code>.
     * @param property propriedade.
     * @param defaultValue valor padrão.
     * @return propriedade.
     */
    public int getInt(String property, String defaultValue){
        return this.getNumber(property, defaultValue).intValue();
    }

    /**
     * Obtém a propriedade do tipo <code>long</code>.
     * @param property propriedade.
     * @param defaultValue valor padrão.
     * @return propriedade.
     */
    public long getLong(String property, String defaultValue){
        return this.getNumber(property, defaultValue).longValue();
    }
    
    /**
     * Obtém a propriedade do tipo <code>double</code>.
     * @param property propriedade.
     * @param defaultValue valor padrão.
     * @return propriedade.
     */
    public double getDouble(String property, String defaultValue){
        return this.getNumber(property, defaultValue).doubleValue();
    }

    /**
     * Obtém a propriedade do tipo <code>float</code>.
     * @param property propriedade.
     * @param defaultValue valor padrão.
     * @return propriedade.
     */
    public float getFloat(String property, String defaultValue){
        return this.getNumber(property, defaultValue).floatValue();
    }

    /**
     * Obtém a propriedade do tipo <code>Number</code>.
     * @param property propriedade.
     * @param defaultValue valor padrão.
     * @return propriedade.
     */
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
