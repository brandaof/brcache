package org.brandao.brcache.collections.fileswapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;

import junit.framework.Assert;
import junit.framework.TestCase;

public class DataBlockOutputStreamTest extends TestCase{

	public void test1() throws IOException{
		DataBlockOutputStream out = new DataBlockOutputStream(8);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		
		Date teste = new Date();
		
		ObjectOutputStream oOut = new ObjectOutputStream(out);
		oOut.writeObject(teste);
		oOut.flush();
		oOut.close();
		
		oOut = new ObjectOutputStream(bout);
		oOut.writeObject(teste);
		oOut.flush();
		oOut.close();
		
		byte[] expected = bout.toByteArray();
		for(int i=0;i<expected.length;i++){
			byte value = out.getBlocks().get(i / 8).getData()[i % 8];
			Assert.assertEquals(expected[i], value);
		}
	}
}
