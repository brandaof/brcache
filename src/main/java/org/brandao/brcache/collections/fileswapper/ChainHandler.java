package org.brandao.brcache.collections.fileswapper;

public interface ChainHandler {

	void save(DataBlock entity);

	void update(DataBlock entity);

	void delete(long id);
	
	DataBlock findById(long id);
	
}
