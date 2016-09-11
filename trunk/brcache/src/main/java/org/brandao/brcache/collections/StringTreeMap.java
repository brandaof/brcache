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

import org.brandao.brcache.collections.treehugemap.StringTreeNodes;

/**
 *
 * @author Brandao
 */
public class StringTreeMap<T> extends TreeMap<String, T> {
    
	private static final long serialVersionUID = 4262873183379962091L;

	public StringTreeMap(
            String id, 
            int maxCapacityNodes,
            double clearFactorNodes, 
            double fragmentFactorNodes,
            Swapper swapNodes,
            int quantitySwaperThreadNodes,
            int subListsNodes,
            int maxCapacityElements,
            double clearFactorElements, 
            double fragmentFactorElements,
            Swapper swapElements,
            int quantitySwaperThreadElements,
            int subListsElements){
        super(id, maxCapacityNodes, clearFactorNodes, fragmentFactorNodes, 
                swapNodes, quantitySwaperThreadNodes, subListsNodes,
                maxCapacityElements, clearFactorElements, fragmentFactorElements, 
                swapElements, quantitySwaperThreadElements, subListsElements, 
                new StringTreeNodes<T>());
    }    
}
