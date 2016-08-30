package org.brandao.brcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class CacheTestHelper {

	public static InputStream toStream(Object o) throws IOException{
		ByteArrayOutputStream bout;
		bout = new ByteArrayOutputStream();
		ObjectOutputStream oout = new ObjectOutputStream(bout);
		oout.writeObject(o);
		oout.flush();
		return new ByteArrayInputStream(bout.toByteArray());
	}

	public static Object toObject(InputStream in) throws IOException, ClassNotFoundException{
		ObjectInputStream oin = new ObjectInputStream(in);
		return oin.readObject();
	}
	
}
