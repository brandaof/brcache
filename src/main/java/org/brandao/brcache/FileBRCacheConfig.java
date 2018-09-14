package org.brandao.brcache;

import java.io.File;
import java.io.FileInputStream;

/**
 * Define a configuração de um cache a partir de um arquivo.
 * <pre>
 * ex:
 *     Configuration configuration = ...;
 *     Cache cache = new Cache(new PropertiesBRCacheConfig(configuration));
 * </pre>
 * @author Brandao
 *
 */
public class FileBRCacheConfig 
	extends PropertiesBRCacheConfig{

	private static final long serialVersionUID = 7110634582865791095L;

	public FileBRCacheConfig(File f) {
		Configuration c = new Configuration();
		FileInputStream fin = null;
		try{
			c.load(fin);
			apply(c);
		}
		catch(Throwable e){
			throw e instanceof IllegalStateException? 
					(IllegalStateException)e : 
					new IllegalStateException(e);
		}
		finally{
			if(fin != null){
				try{
					fin.close();
				}
				catch(Throwable ex){
				}
			}
		}
	}
    
}
