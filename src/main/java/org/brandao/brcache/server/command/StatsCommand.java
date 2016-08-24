package org.brandao.brcache.server.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.brandao.brcache.Cache;
import org.brandao.brcache.server.Terminal;
import org.brandao.brcache.server.TerminalConstants;
import org.brandao.brcache.server.TerminalReader;
import org.brandao.brcache.server.TerminalWriter;
import org.brandao.brcache.server.error.ServerError;
import org.brandao.brcache.server.error.ServerErrorException;
import org.brandao.brcache.server.error.ServerErrors;

public class StatsCommand 
	extends AbstractCommand{

	public void execute(Terminal terminal, Cache cache, TerminalReader reader,
			TerminalWriter writer, String[] parameters)
			throws ServerErrorException {

		try{
			Map<String,Object> map = new HashMap<String, Object>();
			
			Properties config = terminal.getConfiguration();
	
	        for(String prop: config.stringPropertyNames()){
	        	map.put(prop,config.getProperty(prop));
	        }
	
	        map.put("read_entry",   cache.getCountRead());
	        map.put("read_data",    cache.getCountReadData());
	        map.put("write_entry",  cache.getCountWrite());
	        map.put("removed_data", cache.getCountRemoved());
	        
			StringBuilder result = new StringBuilder();
	        
			List<String> keys = new ArrayList<String>(map.keySet());
			Collections.sort(keys);
	        for(String prop: keys){
	            result
	            	.append(prop).append(": ")
	            	.append(map.get(prop)).append(TerminalConstants.CRLFText);
	        }
	        
	        result.append(TerminalConstants.BOUNDARY_MESSAGE);
	        writer.sendMessage(result.toString());
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
