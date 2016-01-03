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

package org.brandao.brcache.collections;

import java.io.Serializable;

/**
 * Define a posição do item na memória.
 * 
 * @author Brandao
 */
public class NodeEntry implements Serializable{

    private Entry index;

    private NodeEntry next;

    private NodeEntry before;
    
    /**
     * Cria uma nova instância.
     * 
     * @param value Índice do item.
     */
    public NodeEntry(Entry value){
        this.index  = value;
        this.next   = null;
        this.before = null;
    }

    /**
     * Obtém o índice do item.
     * @return Índice.
     */
    public Entry getIndex() {
        return index;
    }
    /**
     * Define o índice do item.
     * @param value Índice.
     */
    public void setIndex(Entry value) {
        this.index = value;
    }

    /**
     * Obtém o proximo item.
     * @return Item.
     */
    public NodeEntry getNext() {
        return next;
    }

    /**
     * Define o proximo item.
     * @param next Item.
     */
    public void setNext(NodeEntry next) {
        this.next = next;
    }

    /**
     * Obtém o item anterior.
     * @return Item.
     */
	public NodeEntry getBefore() {
		return before;
	}

	/**
	 * Define o item anterior.
	 * @param before Item.
	 */
	public void setBefore(NodeEntry before) {
		this.before = before;
	}

}
