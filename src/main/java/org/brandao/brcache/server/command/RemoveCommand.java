package org.brandao.brcache.server.command;

import org.brandao.brcache.StreamCache;
import org.brandao.brcache.RecoverException;
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

    public void execute(Terminal terminal, StreamCache cache, TerminalReader reader,
                    TerminalWriter writer, String[] parameters)
                    throws ReadDataException, WriteDataException, ParameterException {

    //if(parameters == null || parameters.length < 2)
    //    throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);

    try {
        cache.remove(parameters[1]);
    }
    catch (RecoverException e) {
        throw new ParameterException(e);
    }

    writer.sendMessage(TerminalConstants.SUCCESS);
    writer.flush();

    }

}
