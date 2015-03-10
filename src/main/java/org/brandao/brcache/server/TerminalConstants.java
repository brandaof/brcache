/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.brandao.brcache.server;

/**
 *
 * @author Cliente
 */
public class TerminalConstants {

    public static final byte[] CRLF = "\r\n".getBytes();
    
    public static final String UNKNOW_COMMAND = "UNKNOW COMMAND: %s";
    
    public static final String INVALID_NUMBER_OF_PARAMETERS = "invalid number of parameters";
    
    public static final String INVALID_TIME = "invalid time";
    
    public static final String SUCCESS = "ok";

    public static final String BOUNDARY = "end";
    
    public static final String INSERT_ENTRY_FAIL = "insert entry fail";
    
    public static final String READ_ENTRY_FAIL = "read entry fail";
    
    public static final String SEND_MESSAGE_FAIL  = "send message fail";
    
    public static final String STREAM_CLOSED = "stream closed";
    
    public static final String DISCONNECT_MESSAGE = "goodbye!";
}
