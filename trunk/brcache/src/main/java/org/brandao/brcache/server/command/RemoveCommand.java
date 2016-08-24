package org.brandao.brcache.server.command;

import java.io.IOException;

import org.brandao.brcache.Cache;
import org.brandao.brcache.StorageException;
import org.brandao.brcache.server.Terminal;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.error.ServerErrorException;
import org.brandao.brcache.server.error.ServerErrors;

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
                    throws ServerErrorException {

	    //if(parameters == null || parameters.length < 2)
	    //    throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);

		boolean result;
	    try {
	        result = cache.remove(parameters[1]);
	    }
	    catch (StorageException e) {
            throw new ServerErrorException(ServerErrors.ERROR_1003, ServerErrors.ERROR_1003.toString("key"));
	    }
    	
    	try{
		    if(result){
		    	writer.sendMessage(TerminalConstants.SUCCESS);
		    }
		    else{
		    	writer.sendMessage(TerminalConstants.NOT_FOUND);
		    }
		    writer.flush();
    	}
    	catch(IOException ex){
            throw new ServerErrorException(ServerErrors.ERROR_1005, ServerErrors.ERROR_1005.toString(), ex);
    	}

    }

}
