package org.brandao.brcache.server.command;

import org.brandao.brcache.server.Command;
import org.brandao.brcache.server.Terminal;

public abstract class AbstractCommand 
	implements Command{

	protected Terminal terminal;
	
	public void setTerminal(Terminal terminal){
		this.terminal = terminal;
	}
	
}
