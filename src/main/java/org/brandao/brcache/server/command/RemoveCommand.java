package org.brandao.brcache.server.command;

import org.brandao.brcache.Cache;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.server.ParameterException;
import org.brandao.brcache.server.ReadDataException;
import org.brandao.brcache.server.Terminal;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.WriteDataException;

/**
 * Representa o comando REMOVE.
 * Sua sintaxe é:
 * DELETE <name> <reserved>\r\n
 * 
 * @author Brandao
 *
 */
public class RemoveCommand extends AbstractCommand{

    public void execute(Terminal terminal, Cache cache, TerminalReader reader,
                    TerminalWriter writer, String[] parameters)
                    throws ReadDataException, WriteDataException, ParameterException {

	    //if(parameters == null || parameters.length < 2)
	    //    throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);
	
		boolean result;
	    try {
	        result = cache.remove(parameters[1]);
	    }
	    catch (StorageException e) {
	        throw new ParameterException(e);
	    }
	
	    if(result){
	    	writer.sendMessage(TerminalConstants.SUCCESS);
	    }
	    else{
	    	writer.sendMessage(TerminalConstants.NOT_FOUND);
	    }
	    writer.flush();

    }

}
