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
 * Encapsula uma entidade. Necessário para sua manipulação.
 * 
 * @author Brandao
 */
public class Entry<T> implements Serializable {

	private static final long serialVersionUID = -9181909539502614706L;

	private Integer index;
    
    private T item;
    
    private Entry<T> next;

    private Entry<T> before;
    
    private boolean needReload;
    
    private NodeEntry node;
    
    private boolean needUpdate;

    public Entry() {
        this.index 		= null;
        this.item 		= null;
        this.needUpdate = false;
    }
    
    /**
     * Cria uma nova instância.
     * 
     * @param index ìndice da entidade.
     * @param update Indica que a entidade sofreu alteração. Verdadeiro indica
     * que a entidade sofreu alteração.
     * @param item Entidade.
     */
    public Entry(Integer index, boolean update, T item) {
        this.index = index;
        this.item = item;
        this.needUpdate = update;
    }
    
    /**
     * Cria uma nova instância.
     * 
     * @param index ìndice da entidade.
     * @param item Entidade.
     */
    public Entry(Integer index, T item) {
        this.index = index;
        this.item = item;
        this.needUpdate = true;
    }

    /**
     * Obtém o índice da entidade.
     * @return Índice.
     */
    public Integer getIndex() {
        return index;
    }

    /**
     * Define o índice da entidade.
     * @param index Índice.
     */
    public void setIndex(Integer index) {
        this.index = index;
    }

    /**
     * Obtém a entidade.
     * @return Entidade.
     */
    public T getItem() {
        return item;
    }

    /**
     * Define a entidade.
     * @param item Entidade.
     */
    public void setItem(T item) {
        this.item = item;
    }

    /**
     * Obtém a posição do item na memória.
     * @return Posição.
     */
    public NodeEntry getNode() {
        return node;
    }

    /**
     * Define a posição do item na memória.
     * @param node Posição
     */
    public void setNode(NodeEntry node) {
        this.node = node;
    }

    /**
     * Verifica se a entidade precisa ser recarregada.
     * @return Verdadeiro se existe a necessidade de ser recarregada. Caso contrário falso.
     */
    public boolean isNeedReload() {
        return needReload;
    }

    /**
     * Define que a entidade precisa ser recarregada.
     * @param value Verdadeiro se existe a necessidade de ser recarregada. Caso contrário falso.
     */
    public void setNeedReload(boolean value) {
        this.needReload = value;
    }

    /**
     * Verifica se a entidade precisa ser atualizada.
     * @return Verdadeiro se existe a necessidade de ser atualizada. Caso contrário falso.
     */
    public boolean isNeedUpdate() {
        return needUpdate;
    }

    /**
     * Define que a entidade precisa ser atualizada.
     * @param needUpdate Verdadeiro se existe a necessidade de ser atualizada. Caso contrário falso.
     */
    public void setNeedUpdate(boolean needUpdate) {
        this.needUpdate = needUpdate;
    }

    /**
     * Obtém o próximo item.
     * @return Item.
     */
    public Entry<T> getNext() {
        return next;
    }

    /**
     * Define o próximo item.
     * @param next Item
     */
    public void setNext(Entry<T> next) {
        this.next = next;
    }

    /**
     * Obtém o item anterior.
     * @return Item.
     */
    public Entry<T> getBefore() {
        return before;
    }

    /**
     * Define o item anterior.
     * @param before Item.
     */
    public void setBefore(Entry<T> before) {
        this.before = before;
    }

}
