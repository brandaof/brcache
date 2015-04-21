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


package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;

/**
 * Fábrica que produz objetos que auxiliam na manipulação do 
 * fluxo de dados de uma conexão.
 * 
 * @author Brandao
 */
public interface StreamFactory {
    /**
     * Aplica as configurações na fábrica.
     * @param config Configuração.
     */
    void setConfiguration(Properties config);
    
    /**
     * Produz um objeto que auxilia na manipulação do fluxo de entrada de uma conexão.
     * @param socket Conexão.
     * @return Objeto.
     * @throws IOException Lançada caso ocorra alguma falha ao tentar produzir o objeto.
     */
    InputStream createInpuStream(Socket socket) throws IOException;
    
    /**
     * Produz um objeto que auxilia na manipulação do fluxo de saída de uma conexão.
     * @param socket Conexão.
     * @return Objeto.
     * @throws IOException Lançada caso ocorra alguma falha ao tentar produzir o objeto.
     */
    OutputStream createOutputStream(Socket socket) throws IOException;
    
}
