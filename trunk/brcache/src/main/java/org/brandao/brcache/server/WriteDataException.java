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
public class WriteDataException extends Exception{
    
    public WriteDataException() {
        super();
    }

    public WriteDataException(String string) {
        super(string);
    }

    public WriteDataException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public WriteDataException(Throwable thrwbl) {
        super(thrwbl);
    }

}
