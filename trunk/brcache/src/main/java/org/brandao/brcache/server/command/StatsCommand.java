package org.brandao.brcache.server.command;

import java.util.Properties;

import org.brandao.brcache.Cache;
import org.brandao.brcache.server.ParameterException;
import org.brandao.brcache.server.ReadDataException;
import org.brandao.brcache.server.Terminal;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.WriteDataException;

public class StatsCommand 
	extends AbstractCommand{

	public void execute(Terminal terminal, Cache cache, TerminalReader reader,
			TerminalWriter writer, String[] parameters)
			throws ReadDataException, WriteDataException, ParameterException {
		
		StringBuilder result = new StringBuilder();
		Properties config = terminal.getConfiguration();
		
        for(String prop: config.stringPropertyNames()){
            result
            	.append(prop).append(": ")
            	.append(config.getProperty(prop)).append(TerminalConstants.CRLFText);
        }
        
        result.append("read_entry: ").append(cache.getCountRead()).append(TerminalConstants.CRLFText);
        result.append("read_data: ").append(cache.getCountReadData()).append(TerminalConstants.CRLFText);
        result.append("write_entry: ").append(cache.getCountWrite()).append(TerminalConstants.CRLFText);
        result.append("write_data: ").append(cache.getCountWriteData()).append(TerminalConstants.CRLFText);
        result.append(TerminalConstants.BOUNDARY_MESSAGE);
        writer.sendMessage(result.toString());
        writer.flush();
	}

}
