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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.brandao.brcache.NonTransactionalCache;
import org.brandao.brcache.client.BrCacheClient;

/**
 * Representa uma coleção de objetos de um determinado tipo. Os objetos dessa
 * coleção são armazenados em cache de forma segmentada.
 * 
 * @author Brandao
 */
public class CacheList<T> 
    implements List<T>,Serializable {

	private static final long serialVersionUID = -617590377196604703L;

	private static NonTransactionalCache cache;
    
    private static BrCacheClient client;

    private final HugeArrayList<T> internalList;
    
    /**
     * Cria uma nova instância.
     * 
     * @param maxCapacityElementsOnMemory Número máximo de item que ficaram em memória.
     * @param swapFactorElements Fator de transferência dos segmentos para o cache.
     * @param fragmentFactorElements Fator de fragmentação da coleção dos itens.
     */
    public CacheList(
            int maxCapacityElementsOnMemory,
            double swapFactorElements, 
            double fragmentFactorElements){
        
        CacheSwapper swap = new CacheSwapper();
        
        this.internalList = 
            new HugeArrayList<T>(
                Collections.getNextId(), 
                maxCapacityElementsOnMemory, 
                swapFactorElements, 
                fragmentFactorElements, 
                swap, 
                0);
        
        this.internalList.setForceSwap(true);
    }
    
    /**
     * @see List#size() 
     * @return 
     */
    public int size() {
        return this.internalList.size();
    }

    /**
     * @see List#isEmpty() 
     * @return 
     */
    public boolean isEmpty() {
        return this.internalList.isEmpty();
    }

    /**
     * @see List#contains(java.lang.Object) 
     * @return 
     */
    public boolean contains(Object o) {
        return this.internalList.contains(o);
    }

    /**
     * @see List#iterator() 
     * @return 
     */
    public Iterator<T> iterator() {
        return this.internalList.iterator();
    }

    /**
     * @see List#toArray() 
     * @return 
     */
    public Object[] toArray() {
        return this.internalList.toArray();
    }

    /**
     * @see List#toArray(T[]) 
     * @return 
     */
    public <K> K[] toArray(K[] a) {
        return this.internalList.toArray(a);
    }

    /**
     * @see List#add(java.lang.Object) 
     * @return 
     */
    public boolean add(T e) {
        return this.internalList.add(e);
    }

    /**
     * @see List#remove(java.lang.Object) 
     * @return 
     */
    public boolean remove(Object o) {
        return this.internalList.remove(o);
    }

    /**
     * @see List#containsAll(java.util.Collection) 
     * @return 
     */
    public boolean containsAll(Collection<?> c) {
        return this.internalList.containsAll(c);
    }

    /**
     * @see List#addAll(java.util.Collection) 
     * @return 
     */
    public boolean addAll(Collection<? extends T> c) {
        return this.internalList.addAll(c);
    }

    /**
     * @see List#addAll(int, java.util.Collection) 
     * @return 
     */
    public boolean addAll(int index, Collection<? extends T> c) {
        return this.addAll(index, c);
    }

    /**
     * @see List#removeAll(java.util.Collection) 
     * @return 
     */
    public boolean removeAll(Collection<?> c) {
        return this.internalList.removeAll(c);
    }

    /**
     * @see List#retainAll(java.util.Collection) 
     * @return 
     */
    public boolean retainAll(Collection<?> c) {
        return this.internalList.removeAll(c);
    }

    /**
     * @see List#clear() 
     */
    public void clear() {
        this.internalList.clear();
    }

    /**
     * @see List#get(int) 
     * @return 
     */
    public T get(int index) {
        return this.internalList.get(index);
    }

    /**
     * @see List#set(int, java.lang.Object) 
     * @return 
     */
    public T set(int index, T element) {
        return this.internalList.set(index, element);
    }

    /**
     * @see List#addAll(int, java.util.Collection) 
     * @return 
     */
    public void add(int index, T element) {
        this.internalList.add(index, element);
    }

    /**
     * @see List#remove(int) 
     * @return 
     */
    public T remove(int index) {
        return this.remove(index);
    }

    /**
     * @see List#indexOf(java.lang.Object) 
     * @return 
     */
    public int indexOf(Object o) {
        return this.internalList.indexOf(o);
    }

    /**
     * @see List#lastIndexOf(java.lang.Object) 
     * @return 
     */
    public int lastIndexOf(Object o) {
        return this.internalList.lastIndexOf(o);
    }

    /**
     * @see List#listIterator() 
     * @return 
     */
    public ListIterator<T> listIterator() {
        return this.internalList.listIterator();
    }

    /**
     * @see List#listIterator(int) 
     * @return 
     */
    public ListIterator<T> listIterator(int index) {
        return this.internalList.listIterator(index);
    }

    /**
     * @see List#subList(int, int) 
     * @return 
     */
    public List<T> subList(int fromIndex, int toIndex) {
        return this.internalList.subList(fromIndex, toIndex);
    }

    /**
     * Obtém o cache associado à coleção.
     * @return Cache
     */
    public static NonTransactionalCache getCache() {
        return cache;
    }

    /**
     * Define o cache associado à coleção.
     * @param aCache Cache.
     */
    public static void setCache(NonTransactionalCache aCache) {
        cache = aCache;
    }

    /**
     * Obtém o cliente do cache associado à coleção
     * @return Cliente.
     */
    public static BrCacheClient getClient() {
        return client;
    }

    /**
     * Define o cliente do cache associado à coleção
     * @param aClient Cliente
     */
    public static void setClient(BrCacheClient aClient) {
        client = aClient;
    }

    /**
     * Define se itens podem ser incluidos, atualizados ou removidos.
     * @param value Verdadeiro para permitir somente a obtenção dos itens.
     * Caso contrário, além de obter, será permitido incluir, atualizar e remover itens.
     */
    public void setReadOnly(boolean value) {
        this.internalList.setReadOnly(value);
    }

    /**
     * Verifica se itens podem ser incluidos, atualizados ou removidos.
     * @return Verdadeiro para permitir somente a obtenção dos itens.
     * Caso contrário, além de obter, será permitido incluir, atualizar e remover itens.
     */
    public boolean isReadOnly() {
        return this.internalList.isReadOnly();
    }

    /**
     * Obtém a identificação da coleção no cache.
     * @return Identificação.
     */
    public String getUniqueId(){
        return this.internalList.getUniqueId();
    }

    /**
     * Envia todos os segmentos que estão em memória para o cache.
     */
    public void flush(){
        this.internalList.flush();
    }
    
    private void writeObject(ObjectOutputStream stream) throws IOException {
        boolean original = this.internalList.isReadOnly();
        try{
            this.internalList.flush();
            this.internalList.setReadOnly(true);
            stream.defaultWriteObject();
        }
        finally{
            this.internalList.setReadOnly(original);
        }
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
    }    
    
}
