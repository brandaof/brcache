/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

/**
 *
 * @author Cliente
 */
public class ParameterException extends Exception{
    
    public ParameterException() {
        super();
    }

    public ParameterException(String string) {
        super(string);
    }

    public ParameterException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public ParameterException(Throwable thrwbl) {
        super(thrwbl);
    }

}
