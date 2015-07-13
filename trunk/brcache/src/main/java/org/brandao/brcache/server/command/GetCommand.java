package org.brandao.brcache.server.command;

import java.io.IOException;
import java.io.OutputStream;

import org.brandao.brcache.Cache;
import org.brandao.brcache.CacheInputStream;
import org.brandao.brcache.RecoverException;
import org.brandao.brcache.server.ParameterException;
import org.brandao.brcache.server.ReadDataException;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.WriteDataException;

/**
 * Representa o comando GET.
 * Sua sintaxe é:
 * GET <nome> <reserved>\r\n
 * 
 * @author Brandao
 *
 */
public class GetCommand extends AbstractCommand{

	public void execute(Cache cache, TerminalReader reader,
			TerminalWriter writer, String[] parameters)
			throws ReadDataException, WriteDataException, ParameterException {

        try{
            if(parameters == null || parameters.length < 1)
                throw new ParameterException(TerminalConstants.INVALID_NUMBER_OF_PARAMETERS);

            CacheInputStream in = null;

            try{
                in = (CacheInputStream) cache.get(parameters[0]);
                if(in != null){
                	writer.sendMessage("VALUE " + parameters[0] + " " + String.valueOf(in.getSize()) + " 0");
                    OutputStream out = null;
                    try{
                        out = writer.getStream();
                        in.transfer(out);
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
                else
                	writer.sendMessage("VALUE " + parameters[0] + " 0 0");
            }
            finally{
                if(in != null)
                    in.close();
            }

            writer.sendMessage(TerminalConstants.BOUNDARY_MESSAGE);
            writer.flush();
        }
        catch(RecoverException e){
            throw new ReadDataException(TerminalConstants.READ_ENTRY_FAIL);
        }		
        catch(IOException e){
            throw new ReadDataException(TerminalConstants.READ_ENTRY_FAIL);
        }		
	}

}
