/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author Cliente
 */
public interface TerminalWriter {
    
    void sendMessage(String message) throws IOException;
    
    OutputStream getStream();
    
}
