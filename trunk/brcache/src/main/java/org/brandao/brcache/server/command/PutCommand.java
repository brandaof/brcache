package org.brandao.brcache.server.command;

import java.io.IOException;
import java.io.InputStream;

import org.brandao.brcache.Cache;
import org.brandao.brcache.server.Terminal;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.error.ServerError;
import org.brandao.brcache.server.error.ServerErrorException;
import org.brandao.brcache.server.error.ServerErrors;

/**
 * Representa o comando PUT.
 * Sua sintaxe é:
 * PUT <name> <time> <size> <reserved>\r\n
 * <data>\r\n
 * END\r\n 
 * @author Brandao
 *
 */
public class PutCommand extends AbstractCommand{

	public void execute(Terminal terminal, Cache cache, TerminalReader reader, TerminalWriter writer,
			String[] parameters) throws ServerErrorException {
		
        int time;
        int size;
        
        try{
            //if(parameters == null || parameters.length < 4)
            //    throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);
            
            try{
                time = Integer.parseInt(parameters[3]);
            }
            catch(NumberFormatException e){
                throw new ServerErrorException(ServerErrors.ERROR_1003, ServerErrors.ERROR_1003.toString("time"));
            }

            try{
                size = Integer.parseInt(parameters[4]);
            }
            catch(NumberFormatException e){
                throw new ServerErrorException(ServerErrors.ERROR_1003, ServerErrors.ERROR_1003.toString("size"));
            }
            
            InputStream stream = null;
            try{
            	stream = reader.getStream(size);
                cache.putStream(
                    parameters[1], 
                    time, 
                    stream);
            }
            finally{
                if(stream != null)
                    stream.close();
            }
            
            String end = reader.getMessage();
            
            if(!TerminalConstants.BOUNDARY_MESSAGE.equals(end)){
                throw new ServerErrorException(ServerErrors.ERROR_1004, ServerErrors.ERROR_1004.toString());
            }
            	
            writer.sendMessage(TerminalConstants.SUCCESS);
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
