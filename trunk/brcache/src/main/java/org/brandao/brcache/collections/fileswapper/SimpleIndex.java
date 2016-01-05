package org.brandao.brcache.collections.fileswapper;

import java.io.IOException;

public class SimpleIndex {

	public SimpleIndex(){
	}
	
	public void registry(String index, long reference, SimpleIndexEntityFile session) throws IOException{
		index = index.toLowerCase();
		SimpleKeyEntity key = new SimpleKeyEntity(index, reference);
		this.persistKey(session, key);
	}

	public void remove(String index, SimpleIndexEntityFile session, String typeName) throws IOException{
		index = index.toLowerCase();
		this.removeKey(session, typeName, index);
	}
	
	public long get(String index, SimpleIndexEntityFile session) throws IOException{
		index = index.toLowerCase();
		SimpleKeyEntity result = this.getKey(session, index);
		return result == null? -1 : result.getReference();
	}
	
	private void persistKey(SimpleIndexEntityFile session, 
			SimpleKeyEntity key) throws IOException{
		char[] chain = key.getIndex().toCharArray();
		
		IndexKey root;
		
		if(session.length() == 0){
			root = new IndexKey();
			root.setId(0);
			session.seek(0);
			session.write(root);
		}
		else{
			session.seek(0);
			root = (IndexKey)session.read();
		}
		
		persistKey(session, root, 0, chain, key);
		
		if(root.isNeedUpdate()){
			root.setNeedUpdate(false);
			session.seek(root.getId());
			session.write(root);
		}
		
	}

	private void persistKey(SimpleIndexEntityFile session,  
			IndexKey indexKey, int pos, char[] chain, SimpleKeyEntity key) throws IOException{
		
		if(pos < chain.length){
			IndexKey next = indexKey.getNextNode(chain[pos], session);
			
			if(next == null){
				next = new IndexKey();
				session.seek(session.length());
				next.setId(session.getOffset());
				session.write(next);
				indexKey.setNextNode(chain[pos], next);
			}
			
			persistKey(session, next, ++pos, chain, key);
			if(next.isNeedUpdate()){
				next.setNeedUpdate(false);
				session.seek(next.getId());
				session.write(next);
			}
		}
		else
			indexKey.setReference(key.getReference());
		
	}
	
	private void removeKey(SimpleIndexEntityFile session, String typeName, String key) throws IOException{
		char[] chain = key.toCharArray();
		
		if(session.length() > 0){
			session.seek(0);
			IndexKey root = (IndexKey) session.read();
			removeKey(session, typeName, root, 0, chain, key);
		}
	}

	private void removeKey(SimpleIndexEntityFile session, String typeName,
			IndexKey indexKey, int pos, char[] chain, String key) throws IOException{
		
		if(pos < chain.length){
			IndexKey next = indexKey.getNextNode(chain[pos], session);
			
			if(next == null)
				return;
			
			removeKey(session, typeName, next, ++pos, chain, key);
		}
		else{
			indexKey.setReference(-1);
			session.seek(indexKey.getId());
			session.write(indexKey);
		}
		
	}
	
	private SimpleKeyEntity getKey(SimpleIndexEntityFile session, String key) throws IOException{
		char[] chain = key.toCharArray();
		
		if(session.length() > 0){
			session.seek(0);
			IndexKey root = (IndexKey) session.read();
			return getKey(session, root, 0, chain, key);
		}
		else
			return null;
	}

	private SimpleKeyEntity getKey(SimpleIndexEntityFile session,
			IndexKey indexKey, int pos, char[] chain, String key) throws IOException{
		
		if(pos < chain.length){
			IndexKey next = indexKey.getNextNode(chain[pos], session);
			
			if(next == null)
				return null;
			
			return getKey(session, next, ++pos, chain, key);
		}
		else{
			SimpleKeyEntity result = new SimpleKeyEntity();
			result.setIndex(key);
			result.setReference(indexKey.getReference());
			return result;
		}
		
	}	
	
}
