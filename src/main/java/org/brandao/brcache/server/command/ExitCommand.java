package org.brandao.brcache.server.command;

import java.io.IOException;

import org.brandao.brcache.Cache;
import org.brandao.brcache.server.ParameterException;
import org.brandao.brcache.server.ReadDataException;
import org.brandao.brcache.server.Terminal;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.WriteDataException;

public class ExitCommand 
	extends AbstractCommand{

	public void execute(Terminal terminal, Cache cache, TerminalReader reader,
			TerminalWriter writer, String[] parameters)
			throws ReadDataException, WriteDataException, ParameterException {
		 
        try{
            writer.sendMessage(TerminalConstants.DISCONNECT);
            writer.flush();
            terminal.destroy();
        }
        catch(IOException e){
        }
	}

}
