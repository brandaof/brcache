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

package org.brandao.brcache.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 *
 * @author Brandao
 */
class BrConnectionInvocationHandler implements InvocationHandler{

    private static final String CLOSE_METHOD = "close";

    private static final String EQUALS_METHOD = "equals";

    private static final String HASHCODE_METHOD = "hashCode";
    
    private BrCacheConnectionPool pool;
    
    private BrCacheConnection connection;
    
    public BrConnectionInvocationHandler(BrCacheConnectionPool pool, BrCacheConnection connection){
        this.pool = pool;
        this.connection = connection;
    }
    
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        
    	String name = method.getName();
        if(name.equals(EQUALS_METHOD)){
        	Object x = args[0];
        	x = x instanceof Proxy? Proxy.getInvocationHandler(x) : x;
            return this.equals(x);
        }
        if(name.equals(HASHCODE_METHOD)){
            return this.hashCode();
        }
        else
        if(!name.equals(CLOSE_METHOD))
            return method.invoke(connection, args);
        else
            return null;
    }
    
}
