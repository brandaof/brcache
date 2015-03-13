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
 * Exceção lançada quando ocorre uma falha ao tentar armazenar um item em um cache.
 * 
 * @author Brandao
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
