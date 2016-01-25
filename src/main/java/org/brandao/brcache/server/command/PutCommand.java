package org.brandao.brcache.server.command;

import java.io.IOException;
import java.io.InputStream;

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
			String[] parameters) throws ReadDataException, WriteDataException,
			ParameterException {
		
        int time;
        int size;
        
        try{
            //if(parameters == null || parameters.length < 4)
            //    throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);
            
            try{
                time = Integer.parseInt(parameters[3]);
            }
            catch(NumberFormatException e){
                throw new ParameterException(TerminalConstants.INVALID_TIME);
            }

            try{
                size = Integer.parseInt(parameters[4]);
            }
            catch(NumberFormatException e){
                throw new ParameterException(TerminalConstants.INVALID_SIZE);
            }
            
            InputStream stream = null;
            try{
            	stream = reader.getStream(size);
                cache.put(
                    parameters[1], 
                    time, 
                    stream);
            }
            finally{
                if(stream != null)
                    stream.close();
            }
            
            String end = reader.getMessage();
            
            if(!TerminalConstants.BOUNDARY_MESSAGE.equals(end))
                throw new ParameterException(TerminalConstants.READ_ENTRY_FAIL);
            	
            writer.sendMessage(TerminalConstants.SUCCESS);
            writer.flush();
        }
        catch (IOException ex) {
            throw new WriteDataException(TerminalConstants.INSERT_ENTRY_FAIL, ex);
        }
        catch(StorageException ex){
            throw new WriteDataException(TerminalConstants.INSERT_ENTRY_FAIL, ex);
        }		
	}

}
