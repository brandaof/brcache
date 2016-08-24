package org.brandao.brcache.server.command;

import java.io.IOException;

import org.brandao.brcache.Cache;
import org.brandao.brcache.server.Terminal;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.error.ServerErrorException;

public class ExitCommand 
	extends AbstractCommand{

	public void execute(Terminal terminal, Cache cache, TerminalReader reader,
			TerminalWriter writer, String[] parameters)
			throws ServerErrorException {
		 
        try{
            writer.sendMessage(TerminalConstants.DISCONNECT);
            writer.flush();
            terminal.destroy();
        }
        catch(IOException e){
        }
	}

}
