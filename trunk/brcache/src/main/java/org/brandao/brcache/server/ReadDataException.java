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
public class ReadDataException extends Exception{
    
    public ReadDataException() {
        super();
    }

    public ReadDataException(String string) {
        super(string);
    }

    public ReadDataException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public ReadDataException(Throwable thrwbl) {
        super(thrwbl);
    }

}
