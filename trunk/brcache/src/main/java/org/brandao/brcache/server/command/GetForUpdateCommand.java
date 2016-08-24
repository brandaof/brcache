package org.brandao.brcache.server.command;

import java.io.IOException;
import java.io.OutputStream;

import org.brandao.brcache.Cache;
import org.brandao.brcache.CacheInputStream;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.TXCache;
import org.brandao.brcache.server.ParameterException;
import org.brandao.brcache.server.ReadDataException;
import org.brandao.brcache.server.Terminal;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.WriteDataException;
import org.brandao.brcache.server.error.ServerError;
import org.brandao.brcache.server.error.ServerErrorException;
import org.brandao.brcache.server.error.ServerErrors;

/**
 * Representa o comando GET.
 * Sua sintaxe é:
 * GET_FOR_UPDATE <nome> <reserved> lock\r\n
 * 
 * @author Brandao
 *
 */
public class GetForUpdateCommand extends AbstractCommand{

	public void execute(Terminal terminal, Cache cache, TerminalReader reader,
			TerminalWriter writer, String[] parameters)
			throws ServerErrorException {

		
		if(!(cache instanceof TXCache)){
			throw new ServerErrorException(ServerErrors.ERROR_1009, ServerErrors.ERROR_1009.toString());
		}
		
		TXCache txCahe = (TXCache)cache;
		
        try{
            //if(parameters == null || parameters.length < 2)
            //    throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);

            CacheInputStream in = null;
            try{
                in = (CacheInputStream) txCahe.get(parameters[1], true);
                if(in != null){
                    String responseMessage = 
                		"VALUE " +
                		parameters[1] +
                		TerminalConstants.SEPARATOR_COMMAND +
                		in.getSize() +
                		" 0";
                    writer.sendMessage(responseMessage);
                    OutputStream out = null;
                    try{
                        out = writer.getStream();
                        in.writeTo(out);
                    }
                    finally{
                        if(out != null){
                            try{
                                out.close();
                            }
                            catch(Throwable e){
                            }
                        }
                        writer.sendCRLF();
                    }
                }
                else{
                    String responseMessage =
                		"VALUE " +
        				parameters[1] +
        				" 0 0";
                    writer.sendMessage(responseMessage);
                }
            }
            finally{
                if(in != null)
                    in.close();
            }

            writer.sendMessage(TerminalConstants.BOUNDARY_MESSAGE);
            writer.flush();
        }
        catch (IOException ex) {
            throw new ServerErrorException(ServerErrors.ERROR_1005, ServerErrors.ERROR_1005.toString(), ex);
        }
        catch(Throwable ex){
        	ServerError error = ServerErrors.getError(ex.getMessage(), ex.getClass());
            throw new ServerErrorException(error, error.toString(), ex);
        }
	}

}
