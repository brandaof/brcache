package org.brandao.brcache.server.command;

import java.util.Properties;

import org.brandao.brcache.Cache;
import org.brandao.brcache.server.ParameterException;
import org.brandao.brcache.server.ReadDataException;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.WriteDataException;

public class StatsCommand 
	extends AbstractCommand{

	public void execute(Cache cache, TerminalReader reader,
			TerminalWriter writer, String[] parameters)
			throws ReadDataException, WriteDataException, ParameterException {
		
		Properties config = this.terminal.getConfiguration();
		
        for(String prop: config.stringPropertyNames())
            writer.sendMessage(prop + ": " + config.getProperty(prop));
        
        writer.sendMessage("read_entry: " + cache.getCountRead());
        writer.sendMessage("read_data: " + cache.getCountReadData());
        writer.sendMessage("write_entry: " + cache.getCountWrite());
        writer.sendMessage("write_data: " + cache.getCountWriteData());
        writer.sendMessage(TerminalConstants.BOUNDARY_MESSAGE);
        writer.flush();
	}

}
