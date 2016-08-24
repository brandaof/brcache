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


/**
 * É a base de uma exceção no <b>BRCache</b>. Lançada se ocorrer alguma falha ao 
 * tentar manipular os dados no cache.
 * 
 * @author Brandao
 */
public class CacheException extends RuntimeException{
    
	private static final long serialVersionUID = -2125449136205991256L;

	private CacheError error;
	
	private Object[] params;
	
    public CacheException() {
    	super();
    }

    public CacheException(String message) {
    	super(message);
    }
    
    public CacheException(Throwable thrwbl, CacheError error, Object ... params) {
        super(error.toString(params), thrwbl);
        this.error = error;
        this.params = params;
    }

    public CacheException(CacheError error, Object ... params) {
        super(error.toString(params));
        this.error = error;
        this.params = params;
    }

	public CacheError getError() {
		return error;
	}

	public Object getParams() {
		return params;
	}

}
