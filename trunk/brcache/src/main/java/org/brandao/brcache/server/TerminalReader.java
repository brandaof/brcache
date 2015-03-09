/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Cliente
 */
public interface TerminalReader {
    
    Command getCommand() throws UnknowCommandException, ReadDataException;
    
    StringBuilder[] getParameters(int size) throws ReadDataException, ParameterException;
    
    InputStream getStream() throws IOException;
    
}
