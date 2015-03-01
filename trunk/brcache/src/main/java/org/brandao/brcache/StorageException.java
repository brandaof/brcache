/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache;

/**
 *
 * @author Cliente
 */
public class StorageException extends Exception{
    
    public StorageException() {
        super();
    }

    public StorageException(String string) {
        super(string);
    }

    public StorageException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public StorageException(Throwable thrwbl) {
        super(thrwbl);
    }

}
