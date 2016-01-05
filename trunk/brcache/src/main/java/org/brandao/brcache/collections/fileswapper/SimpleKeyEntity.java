package org.brandao.brcache.collections.fileswapper;

import java.io.Serializable;

public class SimpleKeyEntity implements Serializable{

	private static final long serialVersionUID = -2613594005923071147L;

	private String index;
	
	private long reference;

	public SimpleKeyEntity() {
	}
	
	public SimpleKeyEntity(String index, long reference) {
		this.index = index;
		this.reference = reference;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public long getReference() {
		return reference;
	}

	public void setReference(long reference) {
		this.reference = reference;
	}
	
	
}
