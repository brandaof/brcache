/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.client;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 *
 * @author Cliente
 */
public class BrConnectionInvocationHandler implements InvocationHandler{

    private static final String CLOSE_METHOD = "close";
    
    private BrCacheConnectionPool pool;
    
    private BrCacheConnection connection;
    
    public BrConnectionInvocationHandler(BrCacheConnectionPool pool, BrCacheConnection connection){
        this.pool = pool;
        this.connection = connection;
    }
    
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        
        if(method.getName().equals(CLOSE_METHOD)){
            pool.release(this.connection);
            this.connection = null;
            return null;
        }
        else{
            try{
               return method.invoke(connection, args);
            }
            catch(Throwable e){
                if(e instanceof IOException){
                    try{
                        pool.shutdown(this.connection);
                    }
                    catch(Throwable ex){
                    }
                }
                throw e;
            }
        }
    }
    
}
