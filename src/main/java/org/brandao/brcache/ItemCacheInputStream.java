package org.brandao.brcache;

import java.io.IOException;
import java.io.InputStream;


/**
 * Representa o fluxo de bytes de um item de um cache 
 * permitindo manipular seus metadados.
 * 
 * @author Brandao.
 *
 */
public class ItemCacheInputStream 
	extends CacheInputStream{

	private InputStream stream;
	
	public ItemCacheInputStream(ItemCacheMetadata metadata, InputStream stream){
		this(
			metadata.getId(),
			metadata.getTimeToLive(), 
			metadata.getTimeToIdle(),
			metadata.getCreationTime(), 
			metadata.getMostRecentTime(), 
			metadata.getFlag(), 
			metadata.getSize(),
			stream);
	}
	
	public ItemCacheInputStream(long id, long timeToLive, 
			long timeToIdle, long creationTime, long mostRecentTime, 
			short flag, long size, InputStream stream){
		super();
		this.setMap(
			new DataMap(id, timeToLive, creationTime, 
					timeToIdle, flag, -1, 0, size, mostRecentTime));
		this.stream = stream;
	}
	
    @Override
    public int read(byte[] bytes, int i, int i1) throws IOException {
        return this.stream.read(bytes, i, i1);
    }
    
    @Override
    public int read() throws IOException {
    	return this.stream.read();
    }
	
}
