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
public class UnknowCommandException extends Exception{
    
    public UnknowCommandException() {
        super();
    }

    public UnknowCommandException(String string) {
        super(string);
    }

    public UnknowCommandException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public UnknowCommandException(Throwable thrwbl) {
        super(thrwbl);
    }

}
